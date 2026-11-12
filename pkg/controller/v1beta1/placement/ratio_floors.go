package placement

import (
	"cmp"
	"fmt"
	"maps"
	"math"
	"math/bits"
	"slices"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/placement/affinity"
	"sigs.k8s.io/ome/pkg/placement/protocol"
)

// plannedBound fixes one component's replica band on a split member. A zero
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

// ceilings resolves each component's cap on one member: the cap the source
// declares for it (see SplitSpec.Ceiling), or for a secondary component
// without one, the primary's cap scaled by its ratio of minimums and rounded
// up. A component without a cap is absent and stays fixed at its share.
func (m splitMinimums) ceilings(source *v1beta1.InferenceService) map[v1beta1.ComponentType]int32 {
	split := splitSpecOf(source)
	primary := split.Ceiling(m.primary)
	out := map[v1beta1.ComponentType]int32{}
	if primary > 0 {
		out[m.primary] = primary
	}
	for component := range m.secondaries() {
		if declared := split.Ceiling(component); declared > 0 {
			out[component] = declared
		} else if primary > 0 {
			out[component] = int32(min(m.scaledCeiling(component, primary), math.MaxInt32))
		}
	}
	return out
}

// splitSpecOf returns the source's split block, or nil.
func splitSpecOf(source *v1beta1.InferenceService) *v1beta1.SplitSpec {
	if source.Spec.Placement == nil {
		return nil
	}
	return source.Spec.Placement.Split
}

// scaledCeiling converts a ceiling declared in primary replicas into a
// secondary component's own ratio, rounded up.
func (m splitMinimums) scaledCeiling(component v1beta1.ComponentType, ceiling int32) int64 {
	return scaleCeil(int64(ceiling), int64(m.floors[component]), int64(m.floors[m.primary]))
}

// scaleCeil scales count by numerator over denominator, rounded up. The
// denominator must be positive.
func scaleCeil(count, numerator, denominator int64) int64 {
	return (count*numerator + denominator - 1) / denominator
}

// splitComponentMinimums reads the source's fleet minimums: the primary
// component's planned count, the decoder's minimum beside an engine primary,
// and the router's minimum when routers are divided across members. A
// secondary minimum declared beside the primary's declared minimum keeps
// their ratio at the planned count (followsPrimary).
func splitComponentMinimums(source *v1beta1.InferenceService) (splitMinimums, error) {
	planned := splitDesiredReplicas(source)
	if planned <= 0 {
		return splitMinimums{}, replicaCountUnresolved(source)
	}
	primary := primaryScaleComponent(declaredComponents(source))
	if primary == "" {
		return splitMinimums{}, fmt.Errorf("placement requires an engine or decoder component")
	}
	out := splitMinimums{primary: primary, floors: map[v1beta1.ComponentType]int32{primary: planned}}
	if primary == v1beta1.EngineComponent && source.Spec.Decoder != nil {
		minimum, err := out.decoderMinimum(source)
		if err != nil {
			return splitMinimums{}, err
		}
		out.floors[v1beta1.DecoderComponent] = minimum
	}
	if routerDivided(source) {
		minimum, err := out.routerMinimum(source)
		if err != nil {
			return splitMinimums{}, err
		}
		out.floors[v1beta1.RouterComponent] = minimum
	}
	return out, nil
}

// decoderMinimum is the decoder's fleet minimum beside an engine primary: the
// declared minimum following the engine's, or one decoder per primary unit
// when none is declared.
func (m splitMinimums) decoderMinimum(source *v1beta1.InferenceService) (int32, error) {
	declared := source.Spec.Decoder.MinReplicas
	if declared == nil {
		return m.floors[m.primary], nil
	}
	if *declared < 0 || int64(*declared) > math.MaxInt32 {
		return 0, fmt.Errorf("decoder.minReplicas must be a nonnegative replica count")
	}
	return m.followsPrimary(int64(*declared), source), nil
}

// routerMinimum is the fleet minimum of routers divided across members: the
// declared minimum following the primary's.
func (m splitMinimums) routerMinimum(source *v1beta1.InferenceService) (int32, error) {
	declared := source.Spec.Router.MinReplicas
	if declared == nil || *declared <= 0 {
		return 0, fmt.Errorf("router.minReplicas must be positive to divide routers across homes")
	}
	return m.followsPrimary(int64(*declared), source), nil
}

// followsPrimary scales a minimum declared beside the primary component's
// declared minimum to the primary's planned count: when the primary declares
// a positive minimum other than the planned count, the minimum keeps their
// ratio, rounded up; otherwise it is used as declared. The result is capped
// at the largest replica count.
func (m splitMinimums) followsPrimary(minimum int64, source *v1beta1.InferenceService) int32 {
	planned := int64(m.floors[m.primary])
	if declared := declaredMinimum(source, m.primary); declared != nil && *declared > 0 && int64(*declared) != planned {
		minimum = scaleCeil(minimum, planned, int64(*declared))
	}
	return int32(min(minimum, math.MaxInt32))
}

// routerDivided reports whether the source divides its routers across Split
// members: no affinity term sets weights, or a term sets a default or router
// weight. Weights that name only the engine or decoder keep the full router
// policy on every member.
func routerDivided(source *v1beta1.InferenceService) bool {
	return source.Spec.Router != nil && placementMode(source) == v1beta1.PlacementModeSplit &&
		affinity.DividesRouters(source.Spec.Placement.ClusterAffinity)
}

// memberShare apportions every secondary component's minimum over the
// members' current primary targets, in the fleet ratio of whatever those targets
// sum to, and returns the named member's floors. The member's own allocation is
// authoritative for its target.
func (m splitMinimums) memberShare(source *v1beta1.InferenceService, candidates []v1beta1.CandidatePlacement, member v1beta1.CandidatePlacement) ([]v1beta1.PlacementComponentFloor, error) {
	if member.Cluster == "" || member.Allocation == nil {
		return nil, fmt.Errorf("split home floors require an identified allocation")
	}
	targets := map[string]int32{}
	allocations := map[string]v1beta1.CandidateAllocationStatus{}
	for _, candidate := range candidates {
		if candidate.Allocation != nil && candidate.Cluster != "" {
			targets[candidate.Cluster] = candidate.Allocation.CurrentReplicas
			allocations[candidate.Cluster] = *candidate.Allocation
		}
	}
	targets[member.Cluster] = member.Allocation.CurrentReplicas
	allocations[member.Cluster] = *member.Allocation
	floors, err := apportionSplitFloors(m, targets, splitWeightsOf(source, allocations))
	if err != nil {
		return nil, err
	}
	return floors[member.Cluster], nil
}

// memberShares returns the named member's minimums twice: trimmed, as
// placement writes them, and as memberShare's in-flight apportionment gives
// them.
//
// While primaries above the minimum are still in flight, the secondary total
// follows them upward. A member whose primary target is not falling gets none
// of that extra: each of its secondary minimums is at most the larger of what
// its copy holds and its share at every member's desired target. Otherwise it
// would start replicas that the move removes once the extra primaries are
// gone. held is nil when the copy was not read, which trims nothing; a copy
// that does not exist yet holds nothing.
func (m splitMinimums) memberShares(source *v1beta1.InferenceService, candidates []v1beta1.CandidatePlacement, member v1beta1.CandidatePlacement, held map[v1beta1.ComponentType]int32) (trimmed, inFlight []v1beta1.PlacementComponentFloor, err error) {
	inFlight, err = m.memberShare(source, candidates, member)
	if err != nil {
		return nil, nil, err
	}
	current := int64(member.Allocation.CurrentReplicas)
	desired := map[string]int32{member.Cluster: member.Allocation.DesiredReplicas}
	allocations := map[string]v1beta1.CandidateAllocationStatus{member.Cluster: *member.Allocation}
	for _, candidate := range candidates {
		if candidate.Allocation != nil && candidate.Cluster != "" && candidate.Cluster != member.Cluster {
			current += int64(candidate.Allocation.CurrentReplicas)
			desired[candidate.Cluster] = candidate.Allocation.DesiredReplicas
			allocations[candidate.Cluster] = *candidate.Allocation
		}
	}
	if held == nil || member.Allocation.CurrentReplicas > member.Allocation.DesiredReplicas || current <= int64(m.floors[m.primary]) {
		return inFlight, inFlight, nil
	}
	// The desired targets passed the plan's own check. Should they fail here,
	// the member keeps its in-flight shares.
	final, err := apportionSplitFloors(m, desired, splitWeightsOf(source, allocations))
	if err != nil {
		return inFlight, inFlight, nil
	}
	finalShares := floorMap(final[member.Cluster])
	trimmed = slices.Clone(inFlight)
	for i, share := range trimmed {
		if share.Component != m.primary {
			trimmed[i].Replicas = min(share.Replicas, max(held[share.Component], finalShares[share.Component]))
		}
	}
	return trimmed, inFlight, nil
}

// splitWeights holds the accepted static weights of a split plan: the primary
// component's, and each other component's that the plan records. Only matched
// members carry weights.
type splitWeights struct {
	primary    map[string]int64
	components map[v1beta1.ComponentType]map[string]int64
	// maxPrimary and maxComponents hold the maximum ratios when the terms set
	// weights; both stay empty for an unweighted source.
	maxPrimary    map[string]int64
	maxComponents map[v1beta1.ComponentType]map[string]int64
}

// splitWeightsOf reads the weights an accepted or proposed allocation records.
// Only static Split gives a component its own weights; every other plan
// apportions by the primary targets alone.
func splitWeightsOf(source *v1beta1.InferenceService, allocations map[string]v1beta1.CandidateAllocationStatus) splitWeights {
	out := splitWeights{
		primary: map[string]int64{}, components: map[v1beta1.ComponentType]map[string]int64{},
		maxPrimary: map[string]int64{}, maxComponents: map[v1beta1.ComponentType]map[string]int64{},
	}
	if placementMode(source) != v1beta1.PlacementModeSplit {
		return out
	}
	for name, allocation := range allocations {
		if allocation.Weight == nil {
			continue
		}
		out.primary[name] = *allocation.Weight
		if allocation.MaxWeight != nil {
			out.maxPrimary[name] = *allocation.MaxWeight
		}
		for _, match := range allocation.ComponentMatches {
			if match.Weight == nil {
				continue
			}
			if out.components[match.Component] == nil {
				out.components[match.Component] = map[string]int64{}
			}
			out.components[match.Component][name] = *match.Weight
			if match.MaxWeight != nil {
				if out.maxComponents[match.Component] == nil {
					out.maxComponents[match.Component] = map[string]int64{}
				}
				out.maxComponents[match.Component][name] = *match.MaxWeight
			}
		}
	}
	return out
}

// ownWeights returns the components whose weights are not proportional to the
// primary component's. They are apportioned by their own weights; every other
// component follows the primary targets.
func (w splitWeights) ownWeights() map[v1beta1.ComponentType]map[string]int64 {
	out := map[v1beta1.ComponentType]map[string]int64{}
	for component, weights := range w.components {
		if !proportionalWeights(w.primary, weights) {
			out[component] = weights
		}
	}
	return out
}

// proportionalWeights reports whether two weight maps differ only by a
// constant positive factor, compared by exact cross-multiplication.
func proportionalWeights(a, b map[string]int64) bool {
	names := map[string]bool{}
	for name := range a {
		names[name] = true
	}
	for name := range b {
		names[name] = true
	}
	var refA, refB int64
	for _, name := range slices.Sorted(maps.Keys(names)) {
		if a[name] > 0 || b[name] > 0 {
			refA, refB = a[name], b[name]
			break
		}
	}
	if refA <= 0 || refB <= 0 {
		return refA == 0 && refB == 0
	}
	for name := range names {
		leftHi, leftLo := bits.Mul64(uint64(max(a[name], 0)), uint64(refB))
		rightHi, rightLo := bits.Mul64(uint64(max(b[name], 0)), uint64(refA))
		if leftHi != rightHi || leftLo != rightLo {
			return false
		}
	}
	return true
}

// servingFloors returns the floors a member's ready count is measured against
// for routing. A component apportioned by its own weights gives members
// different mixes, so every member is then measured against the service's ratio
// instead of its own floors, and traffic follows what each member can serve.
func servingFloors(source *v1beta1.InferenceService, floors map[v1beta1.ComponentType]int32) map[v1beta1.ComponentType]int32 {
	if source == nil || source.Status.Placement == nil {
		return floors
	}
	allocations := map[string]v1beta1.CandidateAllocationStatus{}
	for _, candidate := range source.Status.Placement.Candidates {
		if candidate.Allocation != nil {
			allocations[candidate.Cluster] = *candidate.Allocation
		}
	}
	if len(splitWeightsOf(source, allocations).ownWeights()) == 0 {
		return floors
	}
	minimums, err := splitComponentMinimums(source)
	if err != nil {
		return floors
	}
	return minimums.floors
}

// apportionSplitFloors apportions every secondary component's minimum over the
// members' primary targets. A component without its own weights, or whose
// weights are proportional to the primary component's, follows the primary
// targets. Any other component is apportioned by its own weights over the
// members that hold the primary component, at least one each.
func apportionSplitFloors(minimums splitMinimums, targets map[string]int32, weights splitWeights) (map[string][]v1beta1.PlacementComponentFloor, error) {
	own := weights.ownWeights()
	following := map[v1beta1.ComponentType]int32{}
	for component, minimum := range minimums.secondaries() {
		if _, independent := own[component]; !independent {
			following[component] = minimum
		}
	}
	primaryMinimum := minimums.floors[minimums.primary]
	out, err := protocol.ApportionReplicaFloors(minimums.primary, primaryMinimum, targets, following)
	if err != nil {
		return nil, err
	}
	for _, component := range slices.Sorted(maps.Keys(own)) {
		minimum, declared := minimums.floors[component]
		if !declared || component == minimums.primary {
			continue
		}
		shares, err := protocol.ApportionWeightedReplicaFloor(component, primaryMinimum, targets, minimum, own[component])
		if err != nil {
			return nil, err
		}
		for name := range targets {
			out[name] = append(out[name], v1beta1.PlacementComponentFloor{Component: component, Replicas: shares[name]})
			slices.SortFunc(out[name], func(a, b v1beta1.PlacementComponentFloor) int { return cmp.Compare(a.Component, b.Component) })
		}
	}
	return out, nil
}

// splitPlannedBounds derives the member replica band of each scalable
// component on a split member: its share, up to its cap (see ceilings, or
// ratioCeilings when the terms set replica ratios).
func splitPlannedBounds(source *v1beta1.InferenceService, candidates []v1beta1.CandidatePlacement, member v1beta1.CandidatePlacement, held map[v1beta1.ComponentType]int32) (map[v1beta1.ComponentType]plannedBound, error) {
	minimums, err := splitComponentMinimums(source)
	if err != nil {
		return nil, err
	}
	shares, _, err := minimums.memberShares(source, candidates, member, held)
	if err != nil {
		return nil, err
	}
	var ceilings map[v1beta1.ComponentType]int32
	if usesRatioForm(source) {
		if ceilings, _, err = ratioCeilings(source, candidates, member, shares); err != nil {
			return nil, err
		}
	} else {
		ceilings = minimums.ceilings(source)
	}
	out := make(map[v1beta1.ComponentType]plannedBound, len(shares))
	for _, share := range shares {
		out[share.Component] = plannedBound{share.Replicas, ceilings[share.Component]}
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
	if bound, ok := bounds[v1beta1.RouterComponent]; ok && member.Spec.Router != nil {
		apply(&member.Spec.Router.ComponentExtensionSpec, bound)
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
// component covers count/floor of its share and the member has the smallest
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

// everyRequiredComponentPresent reports whether a member has an instance of every
// component primaryUnits measures: the primary component, and each other
// component without a known zero floor. It is false when the primary floor is
// not positive, because primaryUnits then already counts whole instances.
func everyRequiredComponentPresent(comps []v1beta1.ComponentType, counts map[v1beta1.ComponentType]int32, floors map[v1beta1.ComponentType]int32) bool {
	if len(comps) == 0 || floors[comps[0]] <= 0 {
		return false
	}
	for _, component := range comps {
		if floor, known := floors[component]; known && floor <= 0 {
			continue
		}
		if counts[component] < 1 {
			return false
		}
	}
	return true
}

// occupiedUnits converts one component's occupancy into whole primary units,
// rounding up so a partially consumed unit still counts against the budget.
// Without a positive floor on both sides the raw count is kept.
func occupiedUnits(count, floor, primaryFloor int32) int32 {
	if count <= 0 || floor <= 0 || primaryFloor <= 0 {
		return count
	}
	return int32(min(scaleCeil(int64(count), int64(primaryFloor), int64(floor)), math.MaxInt32))
}
