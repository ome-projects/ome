package routing

import (
	"math"
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"
)

// factor is a small helper to build a *resource.Quantity from a decimal string.
func factor(s string) *resource.Quantity {
	q := resource.MustParse(s)
	return &q
}

func TestWeights(t *testing.T) {
	tests := []struct {
		name  string
		homes []Home
		want  []int32
	}{
		{
			name:  "empty input yields empty output",
			homes: nil,
			want:  []int32{},
		},
		{
			name:  "single healthy home always weight 1",
			homes: []Home{{Cluster: "a", Allocated: 7, Ready: 3}},
			want:  []int32{1},
		},
		{
			name: "equal allocation and factor splits evenly",
			homes: []Home{
				{Cluster: "a", Allocated: 4, Ready: 4},
				{Cluster: "b", Allocated: 4, Ready: 4},
			},
			want: []int32{1, 1},
		},
		{
			name: "proportional to allocation reduced by gcd",
			homes: []Home{
				{Cluster: "a", Allocated: 5, Ready: 5},
				{Cluster: "b", Allocated: 2, Ready: 2},
			},
			want: []int32{5, 2},
		},
		{
			name: "partial readiness lowers each home's share",
			homes: []Home{
				{Cluster: "a", Allocated: 7, Ready: 4},
				{Cluster: "b", Allocated: 3, Ready: 3},
			},
			want: []int32{4, 3},
		},
		{
			name: "heterogeneous hardware weighted by capacity factor",
			homes: []Home{
				{Cluster: "fast", Allocated: 100, Ready: 100, Factor: factor("2")},
				{Cluster: "slow", Allocated: 100, Ready: 100, Factor: factor("1")},
			},
			want: []int32{2, 1},
		},
		{
			name: "fractional factor kept exact via milli scale",
			homes: []Home{
				{Cluster: "fast", Allocated: 100, Ready: 100, Factor: factor("1")},
				{Cluster: "slow", Allocated: 100, Ready: 100, Factor: factor("0.5")},
			},
			want: []int32{2, 1},
		},
		{
			name: "unhealthy home is gated to zero, others keep ratio",
			homes: []Home{
				{Cluster: "a", Allocated: 6, Ready: 6},
				{Cluster: "b", Allocated: 4, Ready: 0},
				{Cluster: "c", Allocated: 2, Ready: 2},
			},
			want: []int32{3, 0, 1},
		},
		{
			name: "all homes unready stay at zero",
			homes: []Home{
				{Cluster: "a", Allocated: 5, Ready: 0},
				{Cluster: "b", Allocated: 3, Ready: 0},
			},
			want: []int32{0, 0},
		},
		{
			name: "nothing allocated stays at zero",
			homes: []Home{
				{Cluster: "a", Allocated: 0, Ready: 4},
				{Cluster: "b", Allocated: 0, Ready: 4},
			},
			want: []int32{0, 0},
		},
		{
			name: "nil factor treated as identity",
			homes: []Home{
				{Cluster: "a", Allocated: 3, Ready: 3, Factor: nil},
				{Cluster: "b", Allocated: 1, Ready: 1, Factor: factor("1")},
			},
			want: []int32{3, 1},
		},
		{
			name: "non-positive factor treated as identity",
			homes: []Home{
				{Cluster: "a", Allocated: 2, Ready: 2, Factor: factor("0")},
				{Cluster: "b", Allocated: 2, Ready: 2, Factor: factor("-3")},
			},
			want: []int32{1, 1},
		},
		{
			name: "heterogeneous with differing allocation and factor",
			homes: []Home{
				// 4 replicas * 3.0 = 12 ; 6 replicas * 1.0 = 6 -> 2:1
				{Cluster: "fast", Allocated: 4, Ready: 4, Factor: factor("3")},
				{Cluster: "slow", Allocated: 6, Ready: 6, Factor: factor("1")},
			},
			want: []int32{2, 1},
		},
		{
			name: "ratio at the publisher limit remains exact",
			homes: []Home{
				{Cluster: "large", Allocated: 1, Ready: 1, Factor: factor("1000000")},
				{Cluster: "baseline", Allocated: 1, Ready: 1, Factor: factor("1")},
			},
			want: []int32{maxTrafficMapWeight, 1},
		},
		{
			name: "ratio one above the publisher limit is proportionally bounded",
			homes: []Home{
				{Cluster: "large", Allocated: 1, Ready: 1, Factor: factor("1000001")},
				{Cluster: "baseline", Allocated: 1, Ready: 1, Factor: factor("1")},
			},
			want: []int32{maxTrafficMapWeight, 1},
		},
		{
			name: "ratio beyond the publisher limit is safely compressed",
			homes: []Home{
				{Cluster: "large", Allocated: 1, Ready: 1, Factor: factor("3000000000")},
				{Cluster: "gated", Allocated: 1, Ready: 0, Factor: factor("9000000000")},
				{Cluster: "baseline", Allocated: 1, Ready: 1, Factor: factor("1")},
			},
			want: []int32{maxTrafficMapWeight, 0, 1},
		},
		{
			name: "factor and allocation multiplication cannot overflow",
			homes: []Home{
				{Cluster: "large", Allocated: math.MaxInt32, Ready: math.MaxInt32, Factor: factor("9223372036854775807")},
				{Cluster: "baseline", Allocated: 1, Ready: 1, Factor: factor("1")},
			},
			want: []int32{maxTrafficMapWeight, 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Weights(tt.homes)
			if len(got) != len(tt.want) {
				t.Fatalf("len(Weights) = %d, want %d (%v)", len(got), len(tt.want), got)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("Weights[%d] = %d, want %d (full: got %v want %v)", i, got[i], tt.want[i], got, tt.want)
				}
			}
		})
	}
}

// ptr is a small helper for the optional observed inputs, whose nil/set
// distinction is the point of the tests below.
func ptr[T any](v T) *T { return &v }

func TestWeights_ReportedCapacityIsACeiling(t *testing.T) {
	tests := []struct {
		name  string
		homes []Home
		want  []int32
	}{
		{
			// The rule that keeps the endpoint input from undoing capacity
			// planning: a home claiming more than the plan gets the plan.
			name: "report above plan does not raise the share",
			homes: []Home{
				{Cluster: "a", Allocated: 7, Ready: 7, Reported: ptr(int32(100))},
				{Cluster: "b", Allocated: 3, Ready: 3},
			},
			want: []int32{7, 3},
		},
		{
			// The rollout-skew case this input exists for: the home knows it has
			// fewer usable prefill/decode pairs than its replica count implies.
			name: "report below plan lowers the share",
			homes: []Home{
				{Cluster: "a", Allocated: 8, Ready: 8, Reported: ptr(int32(2))},
				{Cluster: "b", Allocated: 2, Ready: 2},
			},
			want: []int32{1, 1},
		},
		{
			name: "report equal to plan is a no-op",
			homes: []Home{
				{Cluster: "a", Allocated: 7, Ready: 7, Reported: ptr(int32(7))},
				{Cluster: "b", Allocated: 3, Ready: 3},
			},
			want: []int32{7, 3},
		},
		{
			// Distinct from nil: the home actively says it can serve nothing.
			name: "report of zero drops the home to weight 0",
			homes: []Home{
				{Cluster: "a", Allocated: 7, Ready: 7, Reported: ptr(int32(0))},
				{Cluster: "b", Allocated: 3, Ready: 3},
			},
			want: []int32{0, 1},
		},
		{
			// Fail open: a malformed report must not black-hole a serving home.
			name: "negative report falls back to the plan",
			homes: []Home{
				{Cluster: "a", Allocated: 7, Ready: 7, Reported: ptr(int32(-1))},
				{Cluster: "b", Allocated: 3, Ready: 3},
			},
			want: []int32{7, 3},
		},
		{
			name: "every home reporting zero stays at zero",
			homes: []Home{
				{Cluster: "a", Allocated: 7, Ready: 7, Reported: ptr(int32(0))},
				{Cluster: "b", Allocated: 3, Ready: 3, Reported: ptr(int32(0))},
			},
			want: []int32{0, 0},
		},
		{
			// The ceiling applies to the count, so the factor still scales it.
			name: "ceiling composes with the capacity factor",
			homes: []Home{
				{Cluster: "a", Allocated: 8, Ready: 8, Reported: ptr(int32(2)), Factor: factor("2")},
				{Cluster: "b", Allocated: 4, Ready: 4},
			},
			want: []int32{1, 1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertWeights(t, tt.homes, tt.want)
		})
	}
}

func TestWeights_ProbeGatesOnlyWhenItReachedAVerdict(t *testing.T) {
	tests := []struct {
		name  string
		homes []Home
		want  []int32
	}{
		{
			name: "nil reachable does not gate (probing off, or no verdict yet)",
			homes: []Home{
				{Cluster: "a", Allocated: 7, Ready: 7},
				{Cluster: "b", Allocated: 3, Ready: 3},
			},
			want: []int32{7, 3},
		},
		{
			name: "reachable true is a no-op",
			homes: []Home{
				{Cluster: "a", Allocated: 7, Ready: 7, Reachable: ptr(true)},
				{Cluster: "b", Allocated: 3, Ready: 3, Reachable: ptr(true)},
			},
			want: []int32{7, 3},
		},
		{
			// Ready replicas behind an unreachable path: readiness alone would
			// have kept this home at 70%.
			name: "unreachable home is gated despite being ready",
			homes: []Home{
				{Cluster: "a", Allocated: 7, Ready: 7, Reachable: ptr(false)},
				{Cluster: "b", Allocated: 3, Ready: 3, Reachable: ptr(true)},
			},
			want: []int32{0, 1},
		},
		{
			// PreserveTraffic ignores a fleet-wide probe failure but retains the
			// capacity-derived ratio.
			name: "every home unreachable preserves capacity weights",
			homes: []Home{
				{Cluster: "a", Allocated: 7, Ready: 7, Reachable: ptr(false)},
				{Cluster: "b", Allocated: 3, Ready: 3, Reachable: ptr(false)},
			},
			want: []int32{7, 3},
		},
		{
			// Two independent gates: passing the probe does not excuse having no
			// ready replicas.
			name: "reachable but unready is still gated",
			homes: []Home{
				{Cluster: "a", Allocated: 7, Ready: 0, Reachable: ptr(true)},
				{Cluster: "b", Allocated: 3, Ready: 3, Reachable: ptr(true)},
			},
			want: []int32{0, 1},
		},
		{
			name: "probe gate composes with the reported ceiling",
			homes: []Home{
				{Cluster: "a", Allocated: 8, Ready: 8, Reported: ptr(int32(4))},
				{Cluster: "b", Allocated: 4, Ready: 4, Reachable: ptr(false)},
				{Cluster: "c", Allocated: 4, Ready: 4},
			},
			want: []int32{1, 0, 1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertWeights(t, tt.homes, tt.want)
		})
	}
}

func TestWeights_DrainOnAllProbeFailures(t *testing.T) {
	homes := []Home{
		{Cluster: "a", Allocated: 7, Ready: 7, Reachable: ptr(false)},
		{Cluster: "b", Allocated: 3, Ready: 3, Reachable: ptr(false)},
	}

	assertWeightsWithPolicy(t, homes, AllFailedPolicyDrain, []int32{0, 0})
	assertWeightsWithPolicy(t, homes, AllFailedPolicy(""), []int32{0, 0})
	assertWeightsWithPolicy(t, homes, AllFailedPolicyPreserveTraffic, []int32{7, 3})
}

func TestWeights_PreserveTrafficOnlyIgnoresFleetWideProbeFailure(t *testing.T) {
	tests := []struct {
		name  string
		homes []Home
		want  []int32
	}{
		{
			name: "capacity minima and factors still apply",
			homes: []Home{
				{Cluster: "a", Allocated: 8, Ready: 6, Reported: ptr(int32(4)), Factor: factor("2"), Reachable: ptr(false)},
				{Cluster: "b", Allocated: 6, Ready: 3, Reachable: ptr(false)},
			},
			want: []int32{8, 3},
		},
		{
			name: "unready home stays at zero",
			homes: []Home{
				{Cluster: "a", Allocated: 7, Ready: 0, Reachable: ptr(false)},
				{Cluster: "b", Allocated: 3, Ready: 3, Reachable: ptr(false)},
			},
			want: []int32{0, 1},
		},
		{
			name: "unadmitted home stays at zero",
			homes: []Home{
				{Cluster: "a", Allocated: 0, Ready: 7, Reachable: ptr(false)},
				{Cluster: "b", Allocated: 3, Ready: 3, Reachable: ptr(false)},
			},
			want: []int32{0, 1},
		},
		{
			name: "zero reported capacity stays at zero",
			homes: []Home{
				{Cluster: "a", Allocated: 7, Ready: 7, Reported: ptr(int32(0)), Reachable: ptr(false)},
				{Cluster: "b", Allocated: 3, Ready: 3, Reachable: ptr(false)},
			},
			want: []int32{0, 1},
		},
		{
			name: "all unready stays all zero",
			homes: []Home{
				{Cluster: "a", Allocated: 7, Ready: 0, Reachable: ptr(false)},
				{Cluster: "b", Allocated: 3, Ready: 0, Reachable: ptr(false)},
			},
			want: []int32{0, 0},
		},
		{
			name: "all reported capacity zero stays all zero",
			homes: []Home{
				{Cluster: "a", Allocated: 7, Ready: 7, Reported: ptr(int32(0)), Reachable: ptr(false)},
				{Cluster: "b", Allocated: 3, Ready: 3, Reported: ptr(int32(0)), Reachable: ptr(false)},
			},
			want: []int32{0, 0},
		},
		{
			name: "unknown verdict prevents fallback",
			homes: []Home{
				{Cluster: "a", Allocated: 7, Ready: 7, Reachable: ptr(false)},
				{Cluster: "b", Allocated: 3, Ready: 0},
			},
			want: []int32{0, 0},
		},
		{
			name: "passing verdict prevents fallback",
			homes: []Home{
				{Cluster: "a", Allocated: 7, Ready: 7, Reachable: ptr(false)},
				{Cluster: "b", Allocated: 0, Ready: 3, Reachable: ptr(true)},
			},
			want: []int32{0, 0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertWeightsWithPolicy(t, tt.homes, AllFailedPolicyPreserveTraffic, tt.want)
		})
	}
}

func TestRoutableReplicas(t *testing.T) {
	tests := []struct {
		name string
		home Home
		want int32
	}{
		{
			name: "admitted capacity is the minimum",
			home: Home{Allocated: 3, Ready: 7, Reported: ptr(int32(5))},
			want: 3,
		},
		{
			name: "ready capacity is the minimum",
			home: Home{Allocated: 7, Ready: 3, Reported: ptr(int32(5))},
			want: 3,
		},
		{
			name: "reported capacity is the minimum",
			home: Home{Allocated: 7, Ready: 5, Reported: ptr(int32(2))},
			want: 2,
		},
		{
			name: "absent report leaves admitted and ready minimum",
			home: Home{Allocated: 7, Ready: 3},
			want: 3,
		},
		{
			name: "zero admitted capacity stays zero",
			home: Home{Allocated: 0, Ready: 3},
			want: 0,
		},
		{
			name: "zero ready capacity stays zero",
			home: Home{Allocated: 7, Ready: 0},
			want: 0,
		},
		{
			name: "zero reported capacity stays zero",
			home: Home{Allocated: 7, Ready: 5, Reported: ptr(int32(0))},
			want: 0,
		},
		{
			name: "negative ready capacity stays zero",
			home: Home{Allocated: 7, Ready: -1},
			want: 0,
		},
		{
			name: "negative report is ignored",
			home: Home{Allocated: 7, Ready: 5, Reported: ptr(int32(-1))},
			want: 5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.home.RoutableReplicas(); got != tt.want {
				t.Fatalf("RoutableReplicas() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestEffectiveAllocated(t *testing.T) {
	// The caller records this as the entry's allocation provenance, so it has to
	// agree with what Weights actually used.
	tests := []struct {
		name string
		home Home
		want int32
	}{
		{"no report keeps the plan", Home{Allocated: 7}, 7},
		{"report below plan wins", Home{Allocated: 7, Reported: ptr(int32(3))}, 3},
		{"report above plan is capped at the plan", Home{Allocated: 7, Reported: ptr(int32(9))}, 7},
		{"report of zero is honored", Home{Allocated: 7, Reported: ptr(int32(0))}, 0},
		{"negative report fails open to the plan", Home{Allocated: 7, Reported: ptr(int32(-5))}, 7},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.home.EffectiveAllocated(); got != tt.want {
				t.Fatalf("EffectiveAllocated() = %d, want %d", got, tt.want)
			}
		})
	}
}

func assertWeights(t *testing.T, homes []Home, want []int32) {
	t.Helper()
	got := Weights(homes)
	if len(got) != len(want) {
		t.Fatalf("Weights() length = %d, want %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Weights() = %v, want %v", got, want)
		}
	}
}

func assertWeightsWithPolicy(t *testing.T, homes []Home, policy AllFailedPolicy, want []int32) {
	t.Helper()
	got := weights(homes, policy)
	if len(got) != len(want) {
		t.Fatalf("weights() length = %d, want %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("weights() = %v, want %v", got, want)
		}
	}
}
