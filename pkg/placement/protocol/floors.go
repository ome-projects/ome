package protocol

import (
	"cmp"
	"fmt"
	"maps"
	"math"
	"math/bits"
	"slices"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// ValidateReplicaFloors returns the whole-replica count represented by a full
// home policy: the engine floor when declared, otherwise the decoder floor.
// Engine and decoder floors are independent; router has its own per-home floor.
func ValidateReplicaFloors(floors []v1beta1.PlacementComponentFloor) (int32, error) {
	seen := map[v1beta1.ComponentType]bool{}
	var replicas int32
	var hasReplicaComponent, hasEngine bool
	for _, floor := range floors {
		if (floor.Component != v1beta1.EngineComponent && floor.Component != v1beta1.DecoderComponent && floor.Component != v1beta1.RouterComponent) || seen[floor.Component] || floor.Replicas < 0 {
			return 0, fmt.Errorf("placement replica floors require distinct components and nonnegative counts")
		}
		seen[floor.Component] = true
		if floor.Component == v1beta1.RouterComponent {
			continue
		}
		if floor.Component == v1beta1.EngineComponent || !hasEngine {
			replicas = floor.Replicas
		}
		hasEngine = hasEngine || floor.Component == v1beta1.EngineComponent
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

// ApportionReplicaFloors spreads each secondary component's minimum over homes
// in proportion to their primary targets: largest remainder, ties by home
// name, and at least one replica on every positive-target home whenever the
// minimum is positive. The minimums describe the fleet at primaryMinimum
// primary replicas; targets that sum to another count carry each secondary
// total along in the same ratio, rounded up. A total below the number of
// positive-target homes cannot satisfy the guarantee and is an error. Router
// keeps its own floor.
func ApportionReplicaFloors(primary v1beta1.ComponentType, primaryMinimum int32, targets map[string]int32, minimums map[v1beta1.ComponentType]int32) (map[string][]v1beta1.PlacementComponentFloor, error) {
	if primary != v1beta1.EngineComponent && primary != v1beta1.DecoderComponent {
		return nil, fmt.Errorf("placement primary component must be the engine or decoder")
	}
	if primaryMinimum <= 0 {
		return nil, fmt.Errorf("placement %s minimum must be positive", primary)
	}
	names := slices.Sorted(maps.Keys(targets))
	weights := make(map[string]int64, len(targets))
	positive := 0
	var sum int64
	for _, name := range names {
		if name == "" {
			return nil, fmt.Errorf("placement homes require a name")
		}
		if targets[name] < 0 {
			return nil, fmt.Errorf("placement %s targets must be nonnegative", primary)
		}
		weights[name] = int64(targets[name])
		sum += int64(targets[name])
		if targets[name] > 0 {
			positive++
		}
	}
	out := make(map[string][]v1beta1.PlacementComponentFloor, len(targets))
	for _, name := range names {
		out[name] = []v1beta1.PlacementComponentFloor{{Component: primary, Replicas: targets[name]}}
	}
	for _, component := range slices.Sorted(maps.Keys(minimums)) {
		minimum := minimums[component]
		switch {
		case component == primary:
			return nil, fmt.Errorf("placement primary %s floors are the targets and cannot be apportioned", primary)
		case component == v1beta1.RouterComponent:
			return nil, fmt.Errorf("placement router floors follow the per-home policy and cannot be apportioned")
		case component != v1beta1.EngineComponent && component != v1beta1.DecoderComponent:
			return nil, fmt.Errorf("placement cannot apportion component %q", component)
		case minimum < 0:
			return nil, fmt.Errorf("placement %s minimum must be nonnegative", component)
		}
		total, err := ratioTotal(sum, minimum, primaryMinimum)
		if err != nil {
			return nil, fmt.Errorf("placement %s total: %w", component, err)
		}
		if total > 0 && int(total) < positive {
			return nil, fmt.Errorf("%s minimum %d cannot give each of the %d homes with a positive %s share at least one replica", component, total, positive, primary)
		}
		shares := largestRemainder(total, weights)
		if total > 0 {
			liftZeroShares(shares, weights)
		}
		for _, name := range names {
			out[name] = append(out[name], v1beta1.PlacementComponentFloor{Component: component, Replicas: shares[name]})
		}
	}
	for _, floors := range out {
		slices.SortFunc(floors, func(a, b v1beta1.PlacementComponentFloor) int { return cmp.Compare(a.Component, b.Component) })
	}
	return out, nil
}

// ratioTotal scales a component minimum from the fleet's primary minimum to
// the primary replicas actually targeted, rounding up so the ratio stays a
// floor. Targets at the fleet minimum keep the minimum exactly.
func ratioTotal(targets int64, minimum, primaryMinimum int32) (int32, error) {
	if targets == int64(primaryMinimum) || minimum == 0 {
		return minimum, nil
	}
	hi, lo := bits.Mul64(uint64(targets), uint64(minimum))
	if hi >= uint64(primaryMinimum) {
		return 0, fmt.Errorf("scaled replica count exceeds int64")
	}
	quotient, rest := bits.Div64(hi, lo, uint64(primaryMinimum))
	if rest > 0 {
		quotient++
	}
	if quotient > math.MaxInt32 {
		return 0, fmt.Errorf("scaled replica count exceeds int32")
	}
	return int32(quotient), nil
}

// largestRemainder distributes total by weight, breaking remainder ties by
// name. Zero total weight leaves every share at zero.
func largestRemainder(total int32, weights map[string]int64) map[string]int32 {
	names := slices.Sorted(maps.Keys(weights))
	shares := make(map[string]int32, len(weights))
	var sum int64
	for _, name := range names {
		shares[name] = 0
		sum += weights[name]
	}
	if sum == 0 || total == 0 {
		return shares
	}
	type remainder struct {
		name  string
		value uint64
	}
	remainders := make([]remainder, 0, len(names))
	unassigned := total
	for _, name := range names {
		// The quotient is bounded by total because each weight is at most sum.
		hi, lo := bits.Mul64(uint64(total), uint64(weights[name]))
		quotient, rest := bits.Div64(hi, lo, uint64(sum))
		shares[name] = int32(quotient)
		unassigned -= int32(quotient)
		remainders = append(remainders, remainder{name: name, value: rest})
	}
	slices.SortStableFunc(remainders, func(a, b remainder) int { return cmp.Compare(b.value, a.value) })
	for i := int32(0); i < unassigned; i++ {
		shares[remainders[i].name]++
	}
	return shares
}

// liftZeroShares gives every positive-weight home at least one replica by
// taking one from the largest share, so the total stays exact. The caller has
// checked that the total covers every positive-weight home.
func liftZeroShares(shares map[string]int32, weights map[string]int64) {
	names := slices.Sorted(maps.Keys(shares))
	for _, name := range names {
		if weights[name] <= 0 || shares[name] > 0 {
			continue
		}
		largest := ""
		for _, candidate := range names {
			if shares[candidate] > 1 && (largest == "" || shares[candidate] > shares[largest]) {
				largest = candidate
			}
		}
		if largest == "" {
			return
		}
		shares[largest]--
		shares[name]++
	}
}

// ReplicaUnitRatio reduces the component minimums to the smallest whole
// counts in the same ratio. The result is the shape of one measured capacity
// unit; a zero minimum contributes nothing to that unit.
func ReplicaUnitRatio(minimums map[v1beta1.ComponentType]int32) map[v1beta1.ComponentType]int64 {
	var divisor int64
	for _, minimum := range minimums {
		if minimum > 0 {
			divisor = gcd(divisor, int64(minimum))
		}
	}
	out := make(map[v1beta1.ComponentType]int64, len(minimums))
	for component, minimum := range minimums {
		if minimum > 0 {
			out[component] = int64(minimum) / divisor
		} else {
			out[component] = 0
		}
	}
	return out
}

func gcd(a, b int64) int64 {
	for b != 0 {
		a, b = b, a%b
	}
	return a
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
