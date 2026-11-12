// Package allocation computes replica floors independently of member admission.
package allocation

import (
	"cmp"
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
	total, err := weightTotal(names, weights)
	if err != nil {
		return Plan{}, err
	}
	plan := Plan{Targets: make(map[string]int32, len(weights)), Unassigned: desired}
	for _, name := range names {
		plan.Targets[name] = 0
	}
	if total == 0 {
		return plan, nil
	}
	assignByLargestRemainder(&plan, desired, names, weights, total)
	if err := checkPerClusterLimit(plan, names, maxPerCluster); err != nil {
		return Plan{}, err
	}
	return plan, nil
}

// weightTotal sums the weights in name order and refuses an empty cluster
// name, a negative weight and a sum past int64.
func weightTotal(names []string, weights map[string]int64) (int64, error) {
	var total int64
	for _, name := range names {
		weight := weights[name]
		if name == "" {
			return 0, errors.New("cluster name must not be empty")
		}
		if weight < 0 {
			return 0, fmt.Errorf("cluster %q weight must be nonnegative", name)
		}
		if weight > math.MaxInt64-total {
			return 0, errors.New("sum of cluster weights exceeds int64")
		}
		total += weight
	}
	return total, nil
}

// remainder is the part of a cluster's exact share that whole units do not
// cover; the largest remainders, then the first names, take the units left.
type remainder struct {
	cluster string
	value   uint64
}

// assignByLargestRemainder gives every cluster the whole units of its share
// of desired, then one more unit each to the clusters with the largest
// remainders until nothing is left unassigned.
func assignByLargestRemainder(plan *Plan, desired int32, names []string, weights map[string]int64, total int64) {
	remainders := make([]remainder, 0, len(names))
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
		return cmp.Compare(b.value, a.value)
	})
	for i := int32(0); i < plan.Unassigned; i++ {
		plan.Targets[remainders[i].cluster]++
	}
	plan.Unassigned = 0
}

// checkPerClusterLimit refuses the plan when a positive limit is below any
// cluster's target.
func checkPerClusterLimit(plan Plan, names []string, maxPerCluster int32) error {
	for _, name := range names {
		if maxPerCluster > 0 && plan.Targets[name] > maxPerCluster {
			return &TargetLimitError{Cluster: name, Target: plan.Targets[name], Limit: maxPerCluster}
		}
	}
	return nil
}
