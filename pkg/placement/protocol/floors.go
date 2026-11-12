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

// ValidateReplicaFloors returns the whole-replica count a complete policy for
// one placement cluster represents: the engine minimum when declared,
// otherwise the decoder minimum. Engine and decoder minimums are independent;
// the router has its own per-cluster minimum.
func ValidateReplicaFloors(floors []v1beta1.PlacementComponentFloor) (int32, error) {
	seen := map[v1beta1.ComponentType]bool{}
	var replicas int32
	var hasReplicaComponent, hasEngine bool
	for _, floor := range floors {
		if !IsPlacementComponent(floor.Component) || seen[floor.Component] || floor.Replicas < 0 {
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

// IsPlacementComponent reports whether a component carries a per-cluster
// replica minimum: the engine, the decoder and the router.
func IsPlacementComponent(component v1beta1.ComponentType) bool {
	return component == v1beta1.EngineComponent || component == v1beta1.DecoderComponent || component == v1beta1.RouterComponent
}

// isPrimaryComponent reports whether a component's count can lead a
// placement: the engine or the decoder.
func isPrimaryComponent(component v1beta1.ComponentType) bool {
	return component == v1beta1.EngineComponent || component == v1beta1.DecoderComponent
}

// ValidatePositiveReplicaFloors requires a positive minimum for every
// component before a placement cluster can take part in bounded placement
// movement.
func ValidatePositiveReplicaFloors(floors []v1beta1.PlacementComponentFloor) error {
	if _, err := ValidateReplicaFloors(floors); err != nil {
		return err
	}
	for _, floor := range floors {
		if floor.Replicas == 0 {
			return fmt.Errorf("placement movement requires a positive %s floor", floor.Component)
		}
	}
	return nil
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
// defaults. Zero is an explicit minimum; an omitted count remains unresolved.
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
		if component.spec == nil {
			continue
		}
		if err := CheckComponentReplicaFloor(floors, component.name, component.spec); err != nil {
			return err
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

// ApportionReplicaFloors spreads each secondary component's minimum over the
// placement clusters in proportion to their primary targets: largest
// remainder, ties by cluster name, and at least one replica on every cluster
// with a positive target whenever the minimum is positive. The minimums
// describe the fleet at primaryMinimum primary replicas; targets that sum to
// another count carry each secondary total along in the same ratio, rounded
// up. A total below the number of clusters with a positive target cannot
// satisfy the guarantee and is an error. The router is apportioned only when
// the caller passes its minimum, which a source does when it divides its
// routers across clusters.
func ApportionReplicaFloors(primary v1beta1.ComponentType, primaryMinimum int32, targets map[string]int32, minimums map[v1beta1.ComponentType]int32) (map[string][]v1beta1.PlacementComponentFloor, error) {
	if !isPrimaryComponent(primary) {
		return nil, fmt.Errorf("placement primary component must be the engine or decoder")
	}
	if primaryMinimum <= 0 {
		return nil, fmt.Errorf("placement %s minimum must be positive", primary)
	}
	weights, err := readPrimaryTargets(primary, primaryMinimum, targets)
	if err != nil {
		return nil, err
	}
	out := make(map[string][]v1beta1.PlacementComponentFloor, len(targets))
	for _, name := range weights.names {
		out[name] = []v1beta1.PlacementComponentFloor{{Component: primary, Replicas: targets[name]}}
	}
	for _, component := range slices.Sorted(maps.Keys(minimums)) {
		shares, err := weights.secondaryShares(component, minimums[component])
		if err != nil {
			return nil, err
		}
		for _, name := range weights.names {
			out[name] = append(out[name], v1beta1.PlacementComponentFloor{Component: component, Replicas: shares[name]})
		}
	}
	for _, floors := range out {
		slices.SortFunc(floors, func(a, b v1beta1.PlacementComponentFloor) int { return cmp.Compare(a.Component, b.Component) })
	}
	return out, nil
}

// primaryWeights are the primary targets read as the weights a secondary
// component is spread by: the primary component and the fleet count its
// minimums describe, the cluster names in order, each target as a weight,
// their sum, and how many are positive.
type primaryWeights struct {
	component v1beta1.ComponentType
	minimum   int32
	names     []string
	byName    map[string]int64
	sum       int64
	positive  int
}

// readPrimaryTargets validates the targets in cluster-name order and reads
// them as weights.
func readPrimaryTargets(primary v1beta1.ComponentType, primaryMinimum int32, targets map[string]int32) (primaryWeights, error) {
	weights := primaryWeights{component: primary, minimum: primaryMinimum, names: slices.Sorted(maps.Keys(targets)), byName: make(map[string]int64, len(targets))}
	for _, name := range weights.names {
		if name == "" {
			return primaryWeights{}, fmt.Errorf("placement homes require a name")
		}
		if targets[name] < 0 {
			return primaryWeights{}, fmt.Errorf("placement %s targets must be nonnegative", primary)
		}
		weights.byName[name] = int64(targets[name])
		weights.sum += int64(targets[name])
		if targets[name] > 0 {
			weights.positive++
		}
	}
	return weights, nil
}

// secondaryShares spreads one secondary component's minimum over the primary
// weights: the minimum scaled to the targeted primary count, then largest
// remainder with every positive-target cluster given at least one. The
// primary itself and a component outside the placement set are refused.
func (w primaryWeights) secondaryShares(component v1beta1.ComponentType, minimum int32) (map[string]int32, error) {
	switch {
	case component == w.component:
		return nil, fmt.Errorf("placement primary %s floors are the targets and cannot be apportioned", w.component)
	case !IsPlacementComponent(component):
		return nil, fmt.Errorf("placement cannot apportion component %q", component)
	case minimum < 0:
		return nil, fmt.Errorf("placement %s minimum must be nonnegative", component)
	}
	total, err := ratioTotal(w.sum, minimum, w.minimum)
	if err != nil {
		return nil, fmt.Errorf("placement %s total: %w", component, err)
	}
	if total > 0 && int(total) < w.positive {
		return nil, fmt.Errorf("%s minimum %d cannot give each of the %d homes with a positive %s share at least one replica", component, total, w.positive, w.component)
	}
	return apportionShares(total, w.byName, w.byName), nil
}

// ApportionWeightedReplicaFloor spreads one secondary component's minimum by
// its own weights, over only the placement clusters with a positive primary
// target. The total follows the primary targets as in ApportionReplicaFloors.
// Every such cluster receives at least one replica whenever the total is
// positive, and a total below the number of those clusters is an error. When
// none of those clusters carries a positive weight, the primary targets serve
// as the weights.
func ApportionWeightedReplicaFloor(component v1beta1.ComponentType, primaryMinimum int32, targets map[string]int32, minimum int32, weights map[string]int64) (map[string]int32, error) {
	if !IsPlacementComponent(component) {
		return nil, fmt.Errorf("placement cannot apportion component %q by weight", component)
	}
	if primaryMinimum <= 0 {
		return nil, fmt.Errorf("placement primary minimum must be positive")
	}
	if minimum < 0 {
		return nil, fmt.Errorf("placement %s minimum must be nonnegative", component)
	}
	hosts, err := readWeightedHosts(component, targets, weights)
	if err != nil {
		return nil, err
	}
	total, err := ratioTotal(hosts.targetSum, minimum, primaryMinimum)
	if err != nil {
		return nil, fmt.Errorf("placement %s total: %w", component, err)
	}
	if total > 0 && int(total) < len(hosts.targets) {
		return nil, fmt.Errorf("%s minimum %d cannot give each of the %d homes holding the primary component at least one replica", component, total, len(hosts.targets))
	}
	shares := apportionShares(total, hosts.spreadBy(), hosts.targets)
	out := make(map[string]int32, len(targets))
	for name := range targets {
		out[name] = shares[name]
	}
	return out, nil
}

// weightedHosts are the clusters holding the primary component: each one's
// target and its own weight, the sum of every target (zero targets included)
// and the sum of the weights.
type weightedHosts struct {
	targets   map[string]int64
	weights   map[string]int64
	targetSum int64
	weightSum int64
}

// readWeightedHosts validates every target, and the weight of every cluster
// with a positive target, and reads them.
func readWeightedHosts(component v1beta1.ComponentType, targets map[string]int32, weights map[string]int64) (weightedHosts, error) {
	hosts := weightedHosts{targets: map[string]int64{}, weights: map[string]int64{}}
	for name, target := range targets {
		if name == "" || target < 0 {
			return weightedHosts{}, fmt.Errorf("placement homes require a name and a nonnegative target")
		}
		hosts.targetSum += int64(target)
		if target == 0 {
			continue
		}
		hosts.targets[name] = int64(target)
		weight := weights[name]
		if weight < 0 {
			return weightedHosts{}, fmt.Errorf("placement %s weight for home %q must be nonnegative", component, name)
		}
		if weight > math.MaxInt64-hosts.weightSum {
			return weightedHosts{}, fmt.Errorf("placement %s weights exceed int64", component)
		}
		hosts.weights[name] = weight
		hosts.weightSum += weight
	}
	return hosts, nil
}

// spreadBy is the weights the shares follow: the clusters' own weights, or
// their primary targets when no host carries a positive weight.
func (h weightedHosts) spreadBy() map[string]int64 {
	if h.weightSum == 0 {
		return h.targets
	}
	return h.weights
}

// apportionShares spreads total over the weights by largest remainder and,
// when total is positive, raises every zero share of a cluster with a positive
// stake in hosts to one.
func apportionShares(total int32, weights, hosts map[string]int64) map[string]int32 {
	shares := largestRemainder(total, weights)
	if total > 0 {
		raiseZeroShares(shares, hosts)
	}
	return shares
}

// ratioTotal scales a component minimum from the fleet's primary minimum to
// the primary replicas actually targeted, rounding up so the scaled count
// never falls below the ratio. Targets at the fleet minimum keep the minimum
// exactly.
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

// raiseZeroShares gives every cluster with a positive stake at least one
// replica by taking one from the largest share, so the total stays exact. The
// caller has checked that the total covers every such cluster.
func raiseZeroShares(shares map[string]int32, hosts map[string]int64) {
	names := slices.Sorted(maps.Keys(shares))
	for _, name := range names {
		if hosts[name] <= 0 || shares[name] > 0 {
			continue
		}
		largest := largestShareAboveOne(names, shares)
		if largest == "" {
			return
		}
		shares[largest]--
		shares[name]++
	}
}

// largestShareAboveOne is the first cluster, in name order, whose share is the
// largest of those above one; empty when no share can give one away.
func largestShareAboveOne(names []string, shares map[string]int32) string {
	largest := ""
	for _, candidate := range names {
		if shares[candidate] > 1 && (largest == "" || shares[candidate] > shares[largest]) {
			largest = candidate
		}
	}
	return largest
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
