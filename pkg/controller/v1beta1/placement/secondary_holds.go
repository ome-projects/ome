package placement

import (
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// secondaryMember is one split member's view of a secondary component: the floor
// its member was last given, the floor the current step apportions, and its
// ready instances.
type secondaryMember struct {
	Applied, Next, Ready int32
	// Known means the member was observed; Serving means its ready instances
	// carry traffic.
	Known, Serving bool
}

// secondaryFloorHolds lowers each member's floor toward its next value without
// losing ready instances the fleet still needs. Unready instances above the
// next floor leave at once. Ready ones leave only while ready instances on
// serving members stay at or above the fleet minimum, granted in name order. An
// unobserved member keeps its applied floor. Only floors above Next are returned.
func secondaryFloorHolds(minimum int32, members map[string]secondaryMember) map[string]int32 {
	var serving int64
	for _, member := range members {
		if member.Known && member.Serving {
			serving += int64(member.Ready)
		}
	}
	budget := max(int64(0), serving-int64(minimum))
	out := map[string]int32{}
	for _, name := range slices.Sorted(maps.Keys(members)) {
		member := members[name]
		if member.Applied <= member.Next {
			continue
		}
		floor := member.Applied
		if member.Known {
			floor = max(member.Next, min(member.Applied, member.Ready))
			release := int32(min(int64(floor-member.Next), budget))
			budget -= int64(release)
			floor -= release
		}
		if floor > member.Next {
			out[name] = floor
		}
	}
	return out
}

// splitSecondaryHolds returns, per split member, the secondary floors that stay
// above the step's apportionment until replacements elsewhere are ready. The
// transition gates primary targets on readiness, but a secondary floor also
// falls when the apportionment shifts under an unchanged target. A member with
// no target or asked to drain is retired by the transition and is not held.
func splitSecondaryHolds(source *v1beta1.InferenceService, observations map[string]plannedMemberObservation) (map[string]map[v1beta1.ComponentType]int32, error) {
	if mode := placementMode(source); mode != v1beta1.PlacementModeSplit && mode != v1beta1.PlacementModeSplitByCapacity {
		return nil, nil
	}
	minimums, err := splitComponentMinimums(source)
	if err != nil {
		return nil, err
	}
	secondaries := minimums.secondaries()
	if len(secondaries) == 0 {
		return nil, nil
	}
	candidates := source.Status.Placement.Candidates
	members := map[v1beta1.ComponentType]map[string]secondaryMember{}
	for _, candidate := range candidates {
		assignment := candidate.Allocation
		if assignment == nil || assignment.CurrentHome != nil || assignment.CurrentReplicas <= 0 || assignment.DrainRequested {
			continue
		}
		observed := observations[candidate.Cluster]
		applied := memberFloors(observed.Member)
		floors, _, err := minimums.memberShares(source, candidates, candidate, applied)
		if err != nil {
			return nil, err
		}
		next := floorMap(floors)
		for component := range secondaries {
			if members[component] == nil {
				members[component] = map[string]secondaryMember{}
			}
			members[component][candidate.Cluster] = secondaryMember{
				Applied: applied[component], Next: next[component], Ready: observed.ComponentReady[component],
				Known: observed.MemberState.Known, Serving: observed.MemberState.Routable,
			}
		}
	}
	out := map[string]map[v1beta1.ComponentType]int32{}
	for component, minimum := range secondaries {
		for name, floor := range secondaryFloorHolds(minimum, members[component]) {
			if out[name] == nil {
				out[name] = map[v1beta1.ComponentType]int32{}
			}
			out[name][component] = floor
		}
	}
	return out, nil
}

// memberFloors reads the floors written on a member copy, including the router
// floor that a divided router receives.
func memberFloors(member *v1beta1.InferenceService) map[v1beta1.ComponentType]int32 {
	floors := observedMemberFloors(member)
	if member != nil && member.Spec.Router != nil {
		if floor := member.Spec.Router.MinReplicas; floor != nil && *floor >= 0 && int64(*floor) <= math.MaxInt32 {
			floors[v1beta1.RouterComponent] = int32(*floor)
		}
	}
	return floors
}

// heldFloorsMessage names each held floor for the progress condition.
func heldFloorsMessage(holds map[string]map[v1beta1.ComponentType]int32) string {
	var parts []string
	for _, name := range slices.Sorted(maps.Keys(holds)) {
		for _, component := range slices.Sorted(maps.Keys(holds[name])) {
			parts = append(parts, fmt.Sprintf("%s keeps %d %s replicas", name, holds[name][component], component))
		}
	}
	return strings.Join(parts, "; ") + " until their replacements on other clusters are ready"
}
