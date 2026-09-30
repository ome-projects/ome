package protocol

import (
	"fmt"
	"math"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// ValidateReplicaFloors returns the whole-replica count represented by a full
// home policy. Router consumes its separate per-home floor.
func ValidateReplicaFloors(floors []v1beta1.PlacementComponentFloor) (int32, error) {
	seen := map[v1beta1.ComponentType]bool{}
	var replicas int32
	var hasReplicaComponent bool
	for _, floor := range floors {
		if (floor.Component != v1beta1.EngineComponent && floor.Component != v1beta1.DecoderComponent && floor.Component != v1beta1.RouterComponent) || seen[floor.Component] || floor.Replicas < 0 {
			return 0, fmt.Errorf("placement replica floors require distinct components and nonnegative counts")
		}
		seen[floor.Component] = true
		if floor.Component == v1beta1.RouterComponent {
			continue
		}
		if hasReplicaComponent && replicas != floor.Replicas {
			return 0, fmt.Errorf("placement engine and decoder floors must use equal whole-replica counts")
		}
		replicas = floor.Replicas
		hasReplicaComponent = true
	}
	if !hasReplicaComponent {
		return 0, fmt.Errorf("placement replica floors require an engine or decoder")
	}
	return replicas, nil
}

// ValidatePositiveReplicaFloors requires a guaranteed floor for every component
// before a home can participate in bounded placement movement.
func ValidatePositiveReplicaFloors(floors []v1beta1.PlacementComponentFloor) (int32, error) {
	replicas, err := ValidateReplicaFloors(floors)
	if err != nil {
		return 0, err
	}
	for _, floor := range floors {
		if floor.Replicas == 0 {
			return 0, fmt.Errorf("placement movement requires a positive %s floor", floor.Component)
		}
	}
	return replicas, nil
}

// HasZeroReplicaFloor distinguishes an accepted scale-to-zero contract from
// components whose replica count still uses the local projection policy.
func HasZeroReplicaFloor(policy *v1beta1.PlacementExecutionPolicy, component v1beta1.ComponentType) bool {
	if policy == nil || Validate(policy) != nil {
		return false
	}
	for _, floor := range policy.ReplicaFloors {
		if floor.Component == component {
			return floor.Replicas == 0
		}
	}
	return false
}

// ResolveReplicaFloors reads the resolved component policy without injecting
// defaults. Zero is an explicit floor; an omitted count remains unresolved.
func ResolveReplicaFloors(engine *v1beta1.EngineSpec, decoder *v1beta1.DecoderSpec, router *v1beta1.RouterSpec) ([]v1beta1.PlacementComponentFloor, error) {
	var floors []v1beta1.PlacementComponentFloor
	for _, component := range resolvedFloorComponents(engine, decoder, router) {
		if component.spec == nil {
			continue
		}
		value := component.spec.MinReplicas
		if value == nil || *value < 0 || int64(*value) > math.MaxInt32 {
			return nil, fmt.Errorf("placement %s floor requires an explicitly resolved nonnegative replica count", component.name)
		}
		floors = append(floors, v1beta1.PlacementComponentFloor{Component: component.name, Replicas: int32(*value)})
	}
	if _, err := ValidateReplicaFloors(floors); err != nil {
		return nil, err
	}
	return floors, nil
}

// CheckReplicaFloors verifies the complete resolved component inventory before
// any component may change its workloads or acknowledge placement authority.
func CheckReplicaFloors(floors []v1beta1.PlacementComponentFloor, engine *v1beta1.EngineSpec, decoder *v1beta1.DecoderSpec, router *v1beta1.RouterSpec) error {
	if _, err := ValidateReplicaFloors(floors); err != nil {
		return err
	}
	actual, err := ResolveReplicaFloors(engine, decoder, router)
	if err != nil {
		return err
	}
	if len(actual) != len(floors) {
		return fmt.Errorf("placement replica floors differ from the resolved component inventory")
	}
	for _, component := range resolvedFloorComponents(engine, decoder, router) {
		if component.spec != nil {
			if err := CheckComponentReplicaFloor(floors, component.name, component.spec); err != nil {
				return err
			}
		}
	}
	return nil
}

// CheckComponentReplicaFloor also protects direct component projection paths.
func CheckComponentReplicaFloor(floors []v1beta1.PlacementComponentFloor, component v1beta1.ComponentType, spec *v1beta1.ComponentExtensionSpec) error {
	if _, err := ValidateReplicaFloors(floors); err != nil {
		return err
	}
	for _, floor := range floors {
		if floor.Component == component {
			if spec == nil || spec.MinReplicas == nil || int64(*spec.MinReplicas) != int64(floor.Replicas) {
				return fmt.Errorf("placement %s floor differs from its accepted replica count", component)
			}
			return nil
		}
	}
	return fmt.Errorf("placement replica floors have no %s component", component)
}

type resolvedFloorComponent struct {
	name v1beta1.ComponentType
	spec *v1beta1.ComponentExtensionSpec
}

func resolvedFloorComponents(engine *v1beta1.EngineSpec, decoder *v1beta1.DecoderSpec, router *v1beta1.RouterSpec) []resolvedFloorComponent {
	out := []resolvedFloorComponent{{name: v1beta1.EngineComponent}, {name: v1beta1.DecoderComponent}, {name: v1beta1.RouterComponent}}
	if engine != nil {
		out[0].spec = &engine.ComponentExtensionSpec
	}
	if decoder != nil {
		out[1].spec = &decoder.ComponentExtensionSpec
	}
	if router != nil {
		out[2].spec = &router.ComponentExtensionSpec
	}
	return out
}
