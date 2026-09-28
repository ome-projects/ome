// Package allocation computes replica floors independently of member admission.
package allocation

import (
	"errors"
	"fmt"
	"maps"
	"math"
	"math/bits"
	"slices"
)

// Plan assigns whole replica units to clusters. Zero targets remain explicit.
type Plan struct {
	Targets    map[string]int32
	Unassigned int32
}

// TargetLimitError identifies a ratio that cannot satisfy the declared local ceiling.
type TargetLimitError struct {
	Cluster string
	Target  int32
	Limit   int32
}

func (e *TargetLimitError) Error() string {
	return fmt.Sprintf("cluster %q target %d exceeds per-cluster limit %d", e.Cluster, e.Target, e.Limit)
}

// Apportion distributes desired replicas by largest remainder, breaking ties by
// cluster name. Weights are ratios, not capacity limits. No positive weights
// leaves the entire floor unassigned; a local-cap conflict rejects the whole plan.
func Apportion(desired int32, weights map[string]int64, maxPerCluster int32) (Plan, error) {
	if desired < 0 {
		return Plan{}, errors.New("desired replicas must be nonnegative")
	}
	if maxPerCluster < 0 {
		return Plan{}, errors.New("per-cluster limit must be nonnegative")
	}
	names := slices.Sorted(maps.Keys(weights))
	var total int64
	for _, name := range names {
		weight := weights[name]
		if name == "" {
			return Plan{}, errors.New("cluster name must not be empty")
		}
		if weight < 0 {
			return Plan{}, fmt.Errorf("cluster %q weight must be nonnegative", name)
		}
		if weight > math.MaxInt64-total {
			return Plan{}, errors.New("sum of cluster weights exceeds int64")
		}
		total += weight
	}
	plan := Plan{Targets: make(map[string]int32, len(weights)), Unassigned: desired}
	for _, name := range names {
		plan.Targets[name] = 0
	}
	if total == 0 {
		return plan, nil
	}
	type remainder struct {
		cluster string
		value   uint64
	}
	remainders := make([]remainder, 0, len(weights))
	for _, name := range names {
		// The product may exceed 64 bits, but the quotient is bounded by desired
		// because each weight is at most total. Div64 therefore cannot overflow.
		hi, lo := bits.Mul64(uint64(desired), uint64(weights[name]))
		quotient, rest := bits.Div64(hi, lo, uint64(total))
		plan.Targets[name] = int32(quotient)
		plan.Unassigned -= int32(quotient)
		remainders = append(remainders, remainder{cluster: name, value: rest})
	}
	slices.SortStableFunc(remainders, func(a, b remainder) int {
		switch {
		case a.value > b.value:
			return -1
		case a.value < b.value:
			return 1
		default:
			return 0
		}
	})
	for i := int32(0); i < plan.Unassigned; i++ {
		plan.Targets[remainders[i].cluster]++
	}
	plan.Unassigned = 0
	for _, name := range names {
		if maxPerCluster > 0 && plan.Targets[name] > maxPerCluster {
			return Plan{}, &TargetLimitError{Cluster: name, Target: plan.Targets[name], Limit: maxPerCluster}
		}
	}
	return plan, nil
}
