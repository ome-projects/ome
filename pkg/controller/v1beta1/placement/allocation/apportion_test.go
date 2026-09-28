package allocation

import (
	"encoding/binary"
	"fmt"
	"maps"
	"math"
	"math/big"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestApportionRecipes(t *testing.T) {
	tests := []struct {
		name       string
		desired    int32
		weights    map[string]int64
		want       map[string]int32
		unassigned int32
	}{
		{name: "even", desired: 18, weights: map[string]int64{"a": 1, "b": 1, "c": 1}, want: map[string]int32{"a": 6, "b": 6, "c": 6}},
		{name: "lexical remainder", desired: 10, weights: map[string]int64{"c": 1, "a": 1, "b": 1}, want: map[string]int32{"a": 4, "b": 3, "c": 3}},
		{name: "weighted", desired: 18, weights: map[string]int64{"a": 3, "b": 3, "c": 2, "d": 1}, want: map[string]int32{"a": 6, "b": 6, "c": 4, "d": 2}},
		{name: "weighted remainder", desired: 10, weights: map[string]int64{"a": 3, "b": 3, "c": 2, "d": 1}, want: map[string]int32{"a": 4, "b": 3, "c": 2, "d": 1}},
		{name: "zero target", desired: 2, weights: map[string]int64{"a": 1, "b": 1, "c": 1}, want: map[string]int32{"a": 1, "b": 1, "c": 0}},
		{name: "known zero hardware", desired: 12, weights: map[string]int64{"a": 8, "b": 4, "c": 0}, want: map[string]int32{"a": 8, "b": 4, "c": 0}},
		{name: "hardware does not cap target", desired: 15, weights: map[string]int64{"a": 8, "b": 4, "c": 0}, want: map[string]int32{"a": 10, "b": 5, "c": 0}},
		{name: "fraction outranks name", desired: 2, weights: map[string]int64{"a": 1, "b": 4}, want: map[string]int32{"a": 0, "b": 2}},
		{name: "empty matched set", desired: 7, want: map[string]int32{}, unassigned: 7},
		{name: "all zero hardware", desired: 7, weights: map[string]int64{"a": 0, "b": 0}, want: map[string]int32{"a": 0, "b": 0}, unassigned: 7},
		{name: "zero floor", weights: map[string]int64{"a": 3, "b": 1}, want: map[string]int32{"a": 0, "b": 0}},
		{name: "wide product", desired: math.MaxInt32, weights: map[string]int64{"a": math.MaxInt64}, want: map[string]int32{"a": math.MaxInt32}},
		{name: "wide sum", desired: math.MaxInt32, weights: map[string]int64{"a": math.MaxInt64 / 2, "b": math.MaxInt64 / 2}, want: map[string]int32{"a": 1073741824, "b": 1073741823}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			original := maps.Clone(tt.weights)
			got, err := Apportion(tt.desired, tt.weights, 0)
			require.NoError(t, err)
			require.Equal(t, Plan{Targets: tt.want, Unassigned: tt.unassigned}, got)
			require.Equal(t, original, tt.weights, "allocation must not change matching weights")
		})
	}
}

func TestApportionRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name    string
		desired int32
		weights map[string]int64
		limit   int32
		message string
	}{
		{name: "negative floor", desired: -1, message: "desired replicas"},
		{name: "negative limit", limit: -1, message: "per-cluster limit"},
		{name: "empty cluster", weights: map[string]int64{"": 1}, message: "cluster name"},
		{name: "negative weight", weights: map[string]int64{"a": -1}, message: "weight must be nonnegative"},
		{name: "weight sum overflow", weights: map[string]int64{"a": math.MaxInt64, "b": 1}, message: "sum of cluster weights"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan, err := Apportion(tt.desired, tt.weights, tt.limit)
			require.ErrorContains(t, err, tt.message)
			require.Equal(t, Plan{}, plan, "an invalid plan must not be partially applicable")
		})
	}
}

func TestApportionRejectsCapWithoutRedistribution(t *testing.T) {
	weights := map[string]int64{"a": 3, "b": 1}
	plan, err := Apportion(8, weights, 5)
	var limit *TargetLimitError
	require.ErrorAs(t, err, &limit)
	require.Equal(t, &TargetLimitError{Cluster: "a", Target: 6, Limit: 5}, limit)
	require.ErrorContains(t, err, `cluster "a" target 6 exceeds per-cluster limit 5`)
	require.Equal(t, Plan{}, plan)
	plan, err = Apportion(8, weights, 6)
	require.NoError(t, err)
	require.Equal(t, map[string]int32{"a": 6, "b": 2}, plan.Targets)

	// The limit also applies to replicas awarded by the remainder tie-break.
	plan, err = Apportion(10, map[string]int64{"a": 1, "b": 1, "c": 1}, 3)
	require.ErrorAs(t, err, &limit)
	require.Equal(t, &TargetLimitError{Cluster: "a", Target: 4, Limit: 3}, limit)
	require.Equal(t, Plan{}, plan)
}

func TestApportionWeightsExpressOnlyRatios(t *testing.T) {
	for desired := int32(0); desired <= 100; desired++ {
		base, err := Apportion(desired, map[string]int64{"a": 3, "b": 3, "c": 2, "d": 1}, 0)
		require.NoError(t, err)
		scaled, err := Apportion(desired, map[string]int64{"d": 1000, "b": 3000, "c": 2000, "a": 3000}, 0)
		require.NoError(t, err)
		require.Equal(t, base, scaled)
	}
}

func FuzzApportion(f *testing.F) {
	f.Add(uint32(10), []byte{1, 1, 1})
	f.Add(uint32(18), []byte{3, 3, 2, 1})
	f.Add(uint32(math.MaxInt32), []byte{255, 255, 255, 255, 255, 255, 255, 127})
	f.Add(uint32(7), []byte{0, 0})
	f.Fuzz(func(t *testing.T, desiredBits uint32, data []byte) {
		desired := int32(desiredBits & math.MaxInt32)
		weights := make(map[string]int64)
		// Bound the generated inventory while retaining wide arithmetic cases.
		data = data[:min(len(data), 256)]
		for i := 0; i < len(data); i += 8 {
			var word [8]byte
			copy(word[:], data[i:min(i+8, len(data))])
			weights[fmt.Sprintf("cluster-%03d", i/8)] = int64(binary.LittleEndian.Uint64(word[:]) & math.MaxInt64)
		}
		checkAllocationProperties(t, desired, weights)
	})
}

func checkAllocationProperties(t *testing.T, desired int32, weights map[string]int64) {
	t.Helper()
	total := new(big.Int)
	for _, weight := range weights {
		total.Add(total, big.NewInt(weight))
	}
	plan, err := Apportion(desired, weights, 0)
	if !total.IsInt64() {
		require.ErrorContains(t, err, "sum of cluster weights")
		require.Equal(t, Plan{}, plan)
		return
	}
	require.NoError(t, err)
	require.Len(t, plan.Targets, len(weights))
	var assigned int64
	for name, target := range plan.Targets {
		require.GreaterOrEqual(t, target, int32(0))
		assigned += int64(target)
		if weights[name] == 0 {
			require.Zero(t, target)
		}
	}
	require.Equal(t, int64(desired), assigned+int64(plan.Unassigned))
	if total.Sign() == 0 {
		require.Equal(t, desired, plan.Unassigned)
		return
	}
	require.Zero(t, plan.Unassigned)

	// Exact rationals independently check quota bounds and that every rounded-up
	// cluster outranks every rounded-down cluster, including lexical ties.
	fractions := make(map[string]*big.Rat, len(weights))
	roundedUp := make(map[string]bool, len(weights))
	for name, weight := range weights {
		numerator := new(big.Int).Mul(big.NewInt(int64(desired)), big.NewInt(weight))
		floor := new(big.Int).Quo(numerator, total)
		delta := int64(plan.Targets[name]) - floor.Int64()
		require.True(t, delta == 0 || delta == 1)
		roundedUp[name] = delta == 1
		fractions[name] = new(big.Rat).Sub(new(big.Rat).SetFrac(numerator, total), new(big.Rat).SetInt(floor))
	}
	for a := range weights {
		for b := range weights {
			if roundedUp[a] && !roundedUp[b] {
				comparison := fractions[a].Cmp(fractions[b])
				require.True(t, comparison > 0 || (comparison == 0 && a < b))
			}
		}
	}
	reversed := make(map[string]int64, len(weights))
	names := slices.Sorted(maps.Keys(weights))
	slices.Reverse(names)
	for _, name := range names {
		reversed[name] = weights[name]
	}
	again, err := Apportion(desired, reversed, 0)
	require.NoError(t, err)
	require.Equal(t, plan, again, "inventory ordering must not change the plan")
}
