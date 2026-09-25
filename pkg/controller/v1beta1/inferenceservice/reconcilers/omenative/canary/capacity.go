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
