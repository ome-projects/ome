package canary

import (
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/utils"
)

// resolveStepNewCount resolves a step's Capacity against the component's desired
// replica count, returning the concrete new-revision pod count. Rounds up (so a
// Traffic>0 step always gets at least one new pod) and clamps to [0, desired].
// The old revision runs the complement (desired - newCount) via the partition.
func resolveStepNewCount(step v1beta1.RolloutGroupStep, desired int32) int32 {
	return utils.ClampInt32(utils.ScaledCountFromIntOrString(&step.Capacity, desired, true), 0, desired)
}

// partitionForNewCount maps a desired new-revision count to the StatefulSet-style
// partition the workload reconcile honors: instances with index <
// Partition are held on the old revision, so (desired - newCount) old instances
// are held and newCount roll to the canary revision. Clamped to [0, desired].
func partitionForNewCount(desired, newCount int32) int32 {
	p := desired - newCount
	if p < 0 {
		return 0
	}
	return p
}

// heldFloor is the least number of instances a step keeps on the stable
// revision while the canary is in flight. The stable revision carries traffic
// until the final 100% write lands and drains in-flight requests through the
// drain window after it, so its last serving instance is not released by a
// step; the done sentinel releases it, and the unit reads Stable once that
// instance has rolled. A Component with a single instance has no instance to
// spare and stages in place.
func heldFloor(desired int32) int32 {
	if desired > 1 {
		return 1
	}
	return 0
}

// stepPartition is the partition a step projects while the canary is in
// flight: the complement of its resolved new count, never below the held
// floor.
func stepPartition(step v1beta1.RolloutGroupStep, desired int32) int32 {
	p := partitionForNewCount(desired, resolveStepNewCount(step, desired))
	if floor := heldFloor(desired); p < floor {
		p = floor
	}
	return p
}

// stepStagedCount is the new-revision instance count a step can reach under
// its partition, the count its capacity gate waits for.
func stepStagedCount(step v1beta1.RolloutGroupStep, desired int32) int32 {
	return desired - stepPartition(step, desired)
}
