package coordination

import (
	"fmt"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/utils"
)

// pairFloor is the pairable-capacity floor of one pairing Component: the
// Instances that must keep a serving pod with a partner on the other side
// at every moment of a protocol transition. It is desired minus the
// stricter of the Component's rollingUpdate.maxUnavailable and the group
// pacing's maxUnavailable, the same two caps the unavailability budget is
// built from. A floor of one is the one pair the gate always protects, so
// the floor rule applies from two up (Applies). known is false when the
// replica is missing or records no desired count.
type pairFloor struct {
	known          bool
	desired        int32
	maxUnavailable int32
	floor          int32
}

func (f pairFloor) Applies() bool { return f.known && f.floor >= 2 }

func (f pairFloor) String() string {
	return fmt.Sprintf("floor of %d (desired %d minus maxUnavailable %d)", f.floor, f.desired, f.maxUnavailable)
}

// resolvePairFloor reads the floor off the Component's replica and the
// resolved group pacing.
func resolvePairFloor(ir *v1beta1.InferenceReplica, pacing v1beta1.CoordinationPacing) pairFloor {
	desired := effectiveDesiredReplicas(ir)
	if desired <= 0 {
		return pairFloor{}
	}
	return pairableMinimumUnder(ir, desired, MaxUnavailableBudget(pacing, desired))
}

// ownPairableMinimum reads the pairable minimum off the Component's replica
// alone, for a group that carries no budget of its own (a canary unit):
// desired minus the Component's own maxUnavailable. A Component that
// declares none rolls under no unavailability cap and keeps no minimum
// beyond the one pair the gate always protects.
func ownPairableMinimum(ir *v1beta1.InferenceReplica) pairFloor {
	desired := effectiveDesiredReplicas(ir)
	if desired <= 0 {
		return pairFloor{}
	}
	return pairableMinimumUnder(ir, desired, desired)
}

// pairableMinimumUnder is desired minus the stricter of budget and the
// Component's own rollingUpdate.maxUnavailable.
func pairableMinimumUnder(ir *v1beta1.InferenceReplica, desired, budget int32) pairFloor {
	if lc := ir.Spec.Lifecycle; lc != nil && lc.UpdateStrategy != nil && lc.UpdateStrategy.RollingUpdate != nil && lc.UpdateStrategy.RollingUpdate.MaxUnavailable != nil {
		if own := utils.ScaledCountFromIntOrString(lc.UpdateStrategy.RollingUpdate.MaxUnavailable, desired, true); own < budget {
			budget = own
		}
	}
	floor := desired - budget
	if floor < 0 {
		floor = 0
	}
	return pairFloor{known: true, desired: desired, maxUnavailable: budget, floor: floor}
}

// pairableMinimumOf is the pairable minimum of one pairing side under this
// context's group: desired minus the stricter of the side's own
// maxUnavailable and the group's unavailability budget. A canary unit has
// no group budget, so its members' minimums are their own.
func (ctx GateContext) pairableMinimumOf(ir *v1beta1.InferenceReplica) pairFloor {
	if ctx.CanaryUnit {
		return ownPairableMinimum(ir)
	}
	return resolvePairFloor(ir, ctx.Group.Pacing)
}

// pairableCohortCount counts the serving Instances of one side, held by
// cohort, whose cohort has a partner among the other side's serving
// cohorts.
func pairableCohortCount(side, other map[string]int32) int32 {
	var n int32
	for proto, count := range side {
		if count > 0 && pairsWithPeer(other, proto) {
			n += count
		}
	}
	return n
}

// pairableInstanceCount counts the Instances of one side, each listed with
// the cohorts of its serving pods, that have at least one pod with a
// partner among the other side's serving cohorts.
func pairableInstanceCount(instances map[string][]string, other map[string]int32) int32 {
	var n int32
	for _, protos := range instances {
		for _, proto := range protos {
			if pairsWithPeer(other, proto) {
				n++
				break
			}
		}
	}
	return n
}

// surgeStrategy reports whether a replica updates by SurgeThenDrain, the
// one strategy whose new-cohort pods come up before any old pod leaves. An
// unset strategy is that default; a missing replica reports false.
func surgeStrategy(ir *v1beta1.InferenceReplica) bool {
	if ir == nil {
		return false
	}
	if ir.Spec.Lifecycle == nil || ir.Spec.Lifecycle.UpdateStrategy == nil {
		return true
	}
	switch ir.Spec.Lifecycle.UpdateStrategy.Type {
	case v1beta1.UpdateStrategySurgeThenDrain, "":
		return true
	}
	return false
}
