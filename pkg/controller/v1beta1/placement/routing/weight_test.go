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

func TestWeights_AllocationReadinessAndFactors_ProportionalBoundedRatios(t *testing.T) {
	tests := []struct {
		name     string
		clusters []ClusterCapacity
		want     []int32
	}{
		{
			name:     "empty input yields empty output",
			clusters: nil,
			want:     []int32{},
		},
		{
			name:     "single healthy cluster always weight 1",
			clusters: []ClusterCapacity{{Allocated: 7, Ready: 3}},
			want:     []int32{1},
		},
		{
			name: "equal allocation and factor splits evenly",
			clusters: []ClusterCapacity{
				{Allocated: 4, Ready: 4},
				{Allocated: 4, Ready: 4},
			},
			want: []int32{1, 1},
		},
		{
			name: "proportional to allocation reduced by gcd",
			clusters: []ClusterCapacity{
				{Allocated: 5, Ready: 5},
				{Allocated: 2, Ready: 2},
			},
			want: []int32{5, 2},
		},
		{
			name: "partial readiness lowers each cluster's share",
			clusters: []ClusterCapacity{
				{Allocated: 7, Ready: 4},
				{Allocated: 3, Ready: 3},
			},
			want: []int32{4, 3},
		},
		{
			name: "heterogeneous hardware weighted by capacity factor",
			clusters: []ClusterCapacity{
				{Allocated: 100, Ready: 100, Factor: factor("2")},
				{Allocated: 100, Ready: 100, Factor: factor("1")},
			},
			want: []int32{2, 1},
		},
		{
			name: "fractional factor kept exact via milli scale",
			clusters: []ClusterCapacity{
				{Allocated: 100, Ready: 100, Factor: factor("1")},
				{Allocated: 100, Ready: 100, Factor: factor("0.5")},
			},
			want: []int32{2, 1},
		},
		{
			name: "unhealthy cluster is gated to zero, others keep ratio",
			clusters: []ClusterCapacity{
				{Allocated: 6, Ready: 6},
				{Allocated: 4, Ready: 0},
				{Allocated: 2, Ready: 2},
			},
			want: []int32{3, 0, 1},
		},
		{
			name: "all clusters unready stay at zero",
			clusters: []ClusterCapacity{
				{Allocated: 5, Ready: 0},
				{Allocated: 3, Ready: 0},
			},
			want: []int32{0, 0},
		},
		{
			name: "nothing allocated stays at zero",
			clusters: []ClusterCapacity{
				{Allocated: 0, Ready: 4},
				{Allocated: 0, Ready: 4},
			},
			want: []int32{0, 0},
		},
		{
			name: "nil factor treated as identity",
			clusters: []ClusterCapacity{
				{Allocated: 3, Ready: 3, Factor: nil},
				{Allocated: 1, Ready: 1, Factor: factor("1")},
			},
			want: []int32{3, 1},
		},
		{
			name: "non-positive factor treated as identity",
			clusters: []ClusterCapacity{
				{Allocated: 2, Ready: 2, Factor: factor("0")},
				{Allocated: 2, Ready: 2, Factor: factor("-3")},
			},
			want: []int32{1, 1},
		},
		{
			name: "heterogeneous with differing allocation and factor",
			clusters: []ClusterCapacity{
				// 4 replicas * 3.0 = 12 ; 6 replicas * 1.0 = 6 -> 2:1
				{Allocated: 4, Ready: 4, Factor: factor("3")},
				{Allocated: 6, Ready: 6, Factor: factor("1")},
			},
			want: []int32{2, 1},
		},
		{
			name: "ratio at the publisher limit remains exact",
			clusters: []ClusterCapacity{
				{Allocated: 1, Ready: 1, Factor: factor("1000000")},
				{Allocated: 1, Ready: 1, Factor: factor("1")},
			},
			want: []int32{maxTrafficMapWeight, 1},
		},
		{
			name: "ratio one above the publisher limit is proportionally bounded",
			clusters: []ClusterCapacity{
				{Allocated: 1, Ready: 1, Factor: factor("1000001")},
				{Allocated: 1, Ready: 1, Factor: factor("1")},
			},
			want: []int32{maxTrafficMapWeight, 1},
		},
		{
			name: "ratio beyond the publisher limit is safely compressed",
			clusters: []ClusterCapacity{
				{Allocated: 1, Ready: 1, Factor: factor("3000000000")},
				{Allocated: 1, Ready: 0, Factor: factor("9000000000")},
				{Allocated: 1, Ready: 1, Factor: factor("1")},
			},
			want: []int32{maxTrafficMapWeight, 0, 1},
		},
		{
			name: "factor and allocation multiplication cannot overflow",
			clusters: []ClusterCapacity{
				{Allocated: math.MaxInt32, Ready: math.MaxInt32, Factor: factor("9223372036854775807")},
				{Allocated: 1, Ready: 1, Factor: factor("1")},
			},
			want: []int32{maxTrafficMapWeight, 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := weights(tt.clusters, AllFailedPolicyPreserveTraffic)
			if len(got) != len(tt.want) {
				t.Fatalf("len(weights) = %d, want %d (%v)", len(got), len(tt.want), got)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("weights[%d] = %d, want %d (full: got %v want %v)", i, got[i], tt.want[i], got, tt.want)
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
		name     string
		clusters []ClusterCapacity
		want     []int32
	}{
		{
			// The rule that keeps the endpoint input from undoing capacity
			// planning: a cluster claiming more than the plan gets the plan.
			name: "report above plan does not raise the share",
			clusters: []ClusterCapacity{
				{Allocated: 7, Ready: 7, Reported: ptr(int32(100))},
				{Allocated: 3, Ready: 3},
			},
			want: []int32{7, 3},
		},
		{
			// The rollout-skew case this input exists for: the cluster knows it has
			// fewer usable prefill/decode pairs than its replica count implies.
			name: "report below plan lowers the share",
			clusters: []ClusterCapacity{
				{Allocated: 8, Ready: 8, Reported: ptr(int32(2))},
				{Allocated: 2, Ready: 2},
			},
			want: []int32{1, 1},
		},
		{
			name: "report equal to plan is a no-op",
			clusters: []ClusterCapacity{
				{Allocated: 7, Ready: 7, Reported: ptr(int32(7))},
				{Allocated: 3, Ready: 3},
			},
			want: []int32{7, 3},
		},
		{
			// Distinct from nil: the cluster actively says it can serve nothing.
			name: "report of zero drops the cluster to weight 0",
			clusters: []ClusterCapacity{
				{Allocated: 7, Ready: 7, Reported: ptr(int32(0))},
				{Allocated: 3, Ready: 3},
			},
			want: []int32{0, 1},
		},
		{
			// Fail open: a malformed report must not black-hole a serving cluster.
			name: "negative report falls back to the plan",
			clusters: []ClusterCapacity{
				{Allocated: 7, Ready: 7, Reported: ptr(int32(-1))},
				{Allocated: 3, Ready: 3},
			},
			want: []int32{7, 3},
		},
		{
			name: "every cluster reporting zero stays at zero",
			clusters: []ClusterCapacity{
				{Allocated: 7, Ready: 7, Reported: ptr(int32(0))},
				{Allocated: 3, Ready: 3, Reported: ptr(int32(0))},
			},
			want: []int32{0, 0},
		},
		{
			// The ceiling applies to the count, so the factor still scales it.
			name: "ceiling composes with the capacity factor",
			clusters: []ClusterCapacity{
				{Allocated: 8, Ready: 8, Reported: ptr(int32(2)), Factor: factor("2")},
				{Allocated: 4, Ready: 4},
			},
			want: []int32{1, 1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertWeights(t, tt.clusters, tt.want)
		})
	}
}

func TestWeights_ProbeResultKnownOrNot_GatesOnlyAKnownUnreachable(t *testing.T) {
	tests := []struct {
		name     string
		clusters []ClusterCapacity
		want     []int32
	}{
		{
			name: "nil reachable does not gate (probing off, or no verdict yet)",
			clusters: []ClusterCapacity{
				{Allocated: 7, Ready: 7},
				{Allocated: 3, Ready: 3},
			},
			want: []int32{7, 3},
		},
		{
			name: "reachable true is a no-op",
			clusters: []ClusterCapacity{
				{Allocated: 7, Ready: 7, Reachable: ptr(true)},
				{Allocated: 3, Ready: 3, Reachable: ptr(true)},
			},
			want: []int32{7, 3},
		},
		{
			// Ready replicas behind an unreachable path: readiness alone would
			// have kept this cluster at 70%.
			name: "unreachable cluster is gated despite being ready",
			clusters: []ClusterCapacity{
				{Allocated: 7, Ready: 7, Reachable: ptr(false)},
				{Allocated: 3, Ready: 3, Reachable: ptr(true)},
			},
			want: []int32{0, 1},
		},
		{
			// PreserveTraffic ignores a fleet-wide probe failure but retains the
			// capacity-derived ratio.
			name: "every cluster unreachable preserves capacity weights",
			clusters: []ClusterCapacity{
				{Allocated: 7, Ready: 7, Reachable: ptr(false)},
				{Allocated: 3, Ready: 3, Reachable: ptr(false)},
			},
			want: []int32{7, 3},
		},
		{
			// Two independent gates: passing the probe does not excuse having no
			// ready replicas.
			name: "reachable but unready is still gated",
			clusters: []ClusterCapacity{
				{Allocated: 7, Ready: 0, Reachable: ptr(true)},
				{Allocated: 3, Ready: 3, Reachable: ptr(true)},
			},
			want: []int32{0, 1},
		},
		{
			name: "probe gate composes with the reported ceiling",
			clusters: []ClusterCapacity{
				{Allocated: 8, Ready: 8, Reported: ptr(int32(4))},
				{Allocated: 4, Ready: 4, Reachable: ptr(false)},
				{Allocated: 4, Ready: 4},
			},
			want: []int32{1, 0, 1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertWeights(t, tt.clusters, tt.want)
		})
	}
}

func TestWeights_DrainOnAllProbeFailures(t *testing.T) {
	clusters := []ClusterCapacity{
		{Allocated: 7, Ready: 7, Reachable: ptr(false)},
		{Allocated: 3, Ready: 3, Reachable: ptr(false)},
	}

	assertWeightsWithPolicy(t, clusters, AllFailedPolicyDrain, []int32{0, 0})
	assertWeightsWithPolicy(t, clusters, AllFailedPolicy(""), []int32{0, 0})
	assertWeightsWithPolicy(t, clusters, AllFailedPolicyPreserveTraffic, []int32{7, 3})
}

func TestWeights_PreserveTrafficOnlyIgnoresFleetWideProbeFailure(t *testing.T) {
	tests := []struct {
		name     string
		clusters []ClusterCapacity
		want     []int32
	}{
		{
			name: "capacity minima and factors still apply",
			clusters: []ClusterCapacity{
				{Allocated: 8, Ready: 6, Reported: ptr(int32(4)), Factor: factor("2"), Reachable: ptr(false)},
				{Allocated: 6, Ready: 3, Reachable: ptr(false)},
			},
			want: []int32{8, 3},
		},
		{
			name: "unready cluster stays at zero",
			clusters: []ClusterCapacity{
				{Allocated: 7, Ready: 0, Reachable: ptr(false)},
				{Allocated: 3, Ready: 3, Reachable: ptr(false)},
			},
			want: []int32{0, 1},
		},
		{
			name: "unadmitted cluster stays at zero",
			clusters: []ClusterCapacity{
				{Allocated: 0, Ready: 7, Reachable: ptr(false)},
				{Allocated: 3, Ready: 3, Reachable: ptr(false)},
			},
			want: []int32{0, 1},
		},
		{
			name: "zero reported capacity stays at zero",
			clusters: []ClusterCapacity{
				{Allocated: 7, Ready: 7, Reported: ptr(int32(0)), Reachable: ptr(false)},
				{Allocated: 3, Ready: 3, Reachable: ptr(false)},
			},
			want: []int32{0, 1},
		},
		{
			name: "all unready stays all zero",
			clusters: []ClusterCapacity{
				{Allocated: 7, Ready: 0, Reachable: ptr(false)},
				{Allocated: 3, Ready: 0, Reachable: ptr(false)},
			},
			want: []int32{0, 0},
		},
		{
			name: "all reported capacity zero stays all zero",
			clusters: []ClusterCapacity{
				{Allocated: 7, Ready: 7, Reported: ptr(int32(0)), Reachable: ptr(false)},
				{Allocated: 3, Ready: 3, Reported: ptr(int32(0)), Reachable: ptr(false)},
			},
			want: []int32{0, 0},
		},
		{
			name: "unknown verdict prevents fallback",
			clusters: []ClusterCapacity{
				{Allocated: 7, Ready: 7, Reachable: ptr(false)},
				{Allocated: 3, Ready: 0},
			},
			want: []int32{0, 0},
		},
		{
			name: "passing verdict prevents fallback",
			clusters: []ClusterCapacity{
				{Allocated: 7, Ready: 7, Reachable: ptr(false)},
				{Allocated: 0, Ready: 3, Reachable: ptr(true)},
			},
			want: []int32{0, 0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertWeightsWithPolicy(t, tt.clusters, AllFailedPolicyPreserveTraffic, tt.want)
		})
	}
}

func TestRoutableReplicas_AllocatedReadyAndReported_TakesTheMinimum(t *testing.T) {
	tests := []struct {
		name    string
		cluster ClusterCapacity
		want    int32
	}{
		{
			name:    "admitted capacity is the minimum",
			cluster: ClusterCapacity{Allocated: 3, Ready: 7, Reported: ptr(int32(5))},
			want:    3,
		},
		{
			name:    "ready capacity is the minimum",
			cluster: ClusterCapacity{Allocated: 7, Ready: 3, Reported: ptr(int32(5))},
			want:    3,
		},
		{
			name:    "reported capacity is the minimum",
			cluster: ClusterCapacity{Allocated: 7, Ready: 5, Reported: ptr(int32(2))},
			want:    2,
		},
		{
			name:    "absent report leaves admitted and ready minimum",
			cluster: ClusterCapacity{Allocated: 7, Ready: 3},
			want:    3,
		},
		{
			name:    "zero admitted capacity stays zero",
			cluster: ClusterCapacity{Allocated: 0, Ready: 3},
			want:    0,
		},
		{
			name:    "zero ready capacity stays zero",
			cluster: ClusterCapacity{Allocated: 7, Ready: 0},
			want:    0,
		},
		{
			name:    "zero reported capacity stays zero",
			cluster: ClusterCapacity{Allocated: 7, Ready: 5, Reported: ptr(int32(0))},
			want:    0,
		},
		{
			name:    "negative ready capacity stays zero",
			cluster: ClusterCapacity{Allocated: 7, Ready: -1},
			want:    0,
		},
		{
			name:    "negative report is ignored",
			cluster: ClusterCapacity{Allocated: 7, Ready: 5, Reported: ptr(int32(-1))},
			want:    5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.cluster.RoutableReplicas(); got != tt.want {
				t.Fatalf("RoutableReplicas() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestEffectiveAllocated_ReportBelowAboveOrNegative_CapsAtThePlan(t *testing.T) {
	// The caller records this as the entry's allocation provenance, so it has to
	// agree with what weights used.
	tests := []struct {
		name    string
		cluster ClusterCapacity
		want    int32
	}{
		{"no report keeps the plan", ClusterCapacity{Allocated: 7}, 7},
		{"report below plan wins", ClusterCapacity{Allocated: 7, Reported: ptr(int32(3))}, 3},
		{"report above plan is capped at the plan", ClusterCapacity{Allocated: 7, Reported: ptr(int32(9))}, 7},
		{"report of zero is honored", ClusterCapacity{Allocated: 7, Reported: ptr(int32(0))}, 0},
		{"negative report fails open to the plan", ClusterCapacity{Allocated: 7, Reported: ptr(int32(-5))}, 7},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.cluster.EffectiveAllocated(); got != tt.want {
				t.Fatalf("EffectiveAllocated() = %d, want %d", got, tt.want)
			}
		})
	}
}

func assertWeights(t *testing.T, clusters []ClusterCapacity, want []int32) {
	t.Helper()
	got := weights(clusters, AllFailedPolicyPreserveTraffic)
	if len(got) != len(want) {
		t.Fatalf("weights() length = %d, want %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("weights() = %v, want %v", got, want)
		}
	}
}

func assertWeightsWithPolicy(t *testing.T, clusters []ClusterCapacity, policy AllFailedPolicy, want []int32) {
	t.Helper()
	got := weights(clusters, policy)
	if len(got) != len(want) {
		t.Fatalf("weights() length = %d, want %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("weights() = %v, want %v", got, want)
		}
	}
}
