package placement

import (
	"fmt"
	"math"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/placement/protocol"
)

// plannedBound fixes one component's replica band on a split home. A zero
// ceiling pins the component at its floor.
type plannedBound struct {
	Floor   int32
	Ceiling int32
}

// splitMinimums is the fleet minimum of every scalable component a split
// source apportions, keyed by component, with the component whose instance
// count is the placement unit.
type splitMinimums struct {
	primary v1beta1.ComponentType
	floors  map[v1beta1.ComponentType]int32
}

func (m splitMinimums) secondaries() map[v1beta1.ComponentType]int32 {
	out := map[v1beta1.ComponentType]int32{}
	for component, floor := range m.floors {
		if component != m.primary {
			out[component] = floor
		}
	}
	return out
}

// splitComponentMinimums reads the source's fleet minimums. The primary floor
// is split.replicas or the engine minimum. A declared decoder minimum follows
// the engine minimum in their declared ratio, scaled to the primary floor and
// rounded up; without a declared ratio it is used as declared. An undeclared
// decoder minimum pairs one decoder with each primary unit.
func splitComponentMinimums(source *v1beta1.InferenceService) (splitMinimums, error) {
	primaryFloor := splitDesiredReplicas(source)
	if primaryFloor <= 0 {
		return splitMinimums{}, fmt.Errorf("ReplicaCountUnresolved: declare split.replicas or a positive engine.minReplicas")
	}
	out := splitMinimums{floors: map[v1beta1.ComponentType]int32{}}
	for _, component := range placementScaleComponents(source) {
		if component != v1beta1.EngineComponent && component != v1beta1.DecoderComponent {
			continue
		}
		if out.primary == "" {
			out.primary = component
			out.floors[component] = primaryFloor
			continue
		}
		minimum := int64(primaryFloor)
		if component == v1beta1.DecoderComponent && source.Spec.Decoder.MinReplicas != nil {
			declared := int64(*source.Spec.Decoder.MinReplicas)
			if declared < 0 || declared > math.MaxInt32 {
				return splitMinimums{}, fmt.Errorf("decoder.minReplicas must be a nonnegative replica count")
			}
			minimum = declared
			if engine := source.Spec.Engine.MinReplicas; engine != nil && *engine > 0 && int64(*engine) != int64(primaryFloor) {
				minimum = (declared*int64(primaryFloor) + int64(*engine) - 1) / int64(*engine)
			}
		}
		out.floors[component] = int32(min(minimum, math.MaxInt32))
	}
	if out.primary == "" {
		return splitMinimums{}, fmt.Errorf("placement requires an engine or decoder component")
	}
	return out, nil
}

// splitHomeFloors apportions every secondary component's minimum over the
// homes' current primary targets, in the fleet ratio of whatever those targets
// sum to, and returns the named home's floors. The home's own allocation is
// authoritative for its target.
func splitHomeFloors(source *v1beta1.InferenceService, candidates []v1beta1.CandidatePlacement, home v1beta1.CandidatePlacement) ([]v1beta1.PlacementComponentFloor, error) {
	if home.Cluster == "" || home.Allocation == nil {
		return nil, fmt.Errorf("split home floors require an identified allocation")
	}
	minimums, err := splitComponentMinimums(source)
	if err != nil {
		return nil, err
	}
	targets := map[string]int32{}
	for _, candidate := range candidates {
		if candidate.Allocation != nil && candidate.Cluster != "" {
			targets[candidate.Cluster] = candidate.Allocation.CurrentReplicas
		}
	}
	targets[home.Cluster] = home.Allocation.CurrentReplicas
	floors, err := protocol.ApportionReplicaFloors(minimums.primary, minimums.floors[minimums.primary], targets, minimums.secondaries())
	if err != nil {
		return nil, err
	}
	return floors[home.Cluster], nil
}

// splitPlannedBounds derives the member replica band of each scalable
// component on a split home. The local ceiling is declared in primary
// replicas; a secondary component's ceiling is that ceiling in its own ratio,
// rounded up.
func splitPlannedBounds(source *v1beta1.InferenceService, candidates []v1beta1.CandidatePlacement, home v1beta1.CandidatePlacement) (map[v1beta1.ComponentType]plannedBound, error) {
	floors, err := splitHomeFloors(source, candidates, home)
	if err != nil {
		return nil, err
	}
	minimums, err := splitComponentMinimums(source)
	if err != nil {
		return nil, err
	}
	var ceiling int32
	if source.Spec.Placement != nil && source.Spec.Placement.Split != nil {
		ceiling = source.Spec.Placement.Split.MaxReplicasPerCluster
	}
	out := make(map[v1beta1.ComponentType]plannedBound, len(floors))
	primaryMinimum := int64(minimums.floors[minimums.primary])
	for _, floor := range floors {
		bound := plannedBound{Floor: floor.Replicas, Ceiling: ceiling}
		if ceiling > 0 && floor.Component != minimums.primary {
			scaled := (int64(ceiling)*int64(minimums.floors[floor.Component]) + primaryMinimum - 1) / primaryMinimum
			bound.Ceiling = int32(min(scaled, math.MaxInt32))
		}
		out[floor.Component] = bound
	}
	return out, nil
}

// setPlannedReplicas fixes member bounds at the allocation unless an explicit
// local ceiling permits autoscaling. A retained floor can exceed a reduced cap
// until its replacement serves. Components without a bound keep their policy.
func setPlannedReplicas(member *v1beta1.InferenceService, bounds map[v1beta1.ComponentType]plannedBound) {
	apply := func(component *v1beta1.ComponentExtensionSpec, bound plannedBound) {
		floor := int(bound.Floor)
		component.MinReplicas = &floor
		if bound.Ceiling > 0 {
			component.MaxReplicas = int(max(bound.Ceiling, bound.Floor))
		} else {
			component.MaxReplicas = floor
		}
	}
	if bound, ok := bounds[v1beta1.EngineComponent]; ok && member.Spec.Engine != nil {
		apply(&member.Spec.Engine.ComponentExtensionSpec, bound)
	}
	if bound, ok := bounds[v1beta1.DecoderComponent]; ok && member.Spec.Decoder != nil {
		apply(&member.Spec.Decoder.ComponentExtensionSpec, bound)
	}
}

// floorMap indexes accepted floors by component.
func floorMap(floors []v1beta1.PlacementComponentFloor) map[v1beta1.ComponentType]int32 {
	if len(floors) == 0 {
		return nil
	}
	out := make(map[v1beta1.ComponentType]int32, len(floors))
	for _, floor := range floors {
		out[floor.Component] = floor.Replicas
	}
	return out
}

// primaryUnits expresses per-component counts in whole primary units: each
// component covers count/floor of its share and the home has the smallest
// coverage. Equal or unknown floors reduce to the minimum count; a component
// with a zero floor is skipped.
func primaryUnits(comps []v1beta1.ComponentType, counts map[v1beta1.ComponentType]int32, floors map[v1beta1.ComponentType]int32) int32 {
	if len(comps) == 0 {
		return 0
	}
	primaryFloor := floors[comps[0]]
	units := int32(math.MaxInt32)
	for _, component := range comps {
		count := counts[component]
		floor, known := floors[component]
		switch {
		case primaryFloor <= 0 || !known:
			units = min(units, count)
		case floor <= 0:
			continue
		default:
			units = min(units, int32(min(int64(count)*int64(primaryFloor)/int64(floor), math.MaxInt32)))
		}
	}
	return units
}

// occupiedUnits converts one component's occupancy into whole primary units,
// rounding up so a partially consumed unit still counts against the budget.
// Without a positive floor on both sides the raw count is kept.
func occupiedUnits(count, floor, primaryFloor int32) int32 {
	if count <= 0 || floor <= 0 || primaryFloor <= 0 {
		return count
	}
	return int32(min((int64(count)*int64(primaryFloor)+int64(floor)-1)/int64(floor), math.MaxInt32))
}
