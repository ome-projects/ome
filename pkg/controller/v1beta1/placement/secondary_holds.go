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

// splitSecondaryHolds returns, per split member, the secondary minimums that
// stay above the step's apportionment until replacements elsewhere are
// ready. The transition gates primary targets on readiness, but a secondary
// minimum also falls when the apportionment shifts under an unchanged
// target. A member with no target or asked to drain is retired by the
// transition and is not held.
func splitSecondaryHolds(source *v1beta1.InferenceService, observations map[string]plannedMemberObservation) (map[string]map[v1beta1.ComponentType]int32, error) {
	if !dividesAcrossMembers(source) {
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
	members, err := secondaryMembersOf(source, minimums, secondaries, observations)
	if err != nil {
		return nil, err
	}
	return secondaryHoldsPerMember(secondaries, members), nil
}

// dividesAcrossMembers reports whether the source's placement mode
// apportions component minimums over several members.
func dividesAcrossMembers(source *v1beta1.InferenceService) bool {
	mode := placementMode(source)
	return mode == v1beta1.PlacementModeSplit || mode == v1beta1.PlacementModeSplitByCapacity
}

// standsOnItsShare reports whether an allocation is a split member the step
// keeps: it runs replicas under its share, not under a full policy (one that
// runs the whole service), and is not asked to drain.
func standsOnItsShare(assignment *v1beta1.CandidateAllocationStatus) bool {
	return assignment != nil && assignment.CurrentHome == nil && assignment.CurrentReplicas > 0 && !assignment.DrainRequested
}

// secondaryMembersOf builds, per secondary component, every standing
// member's view: the minimum its copy carries, the one the step apportions,
// and its ready instances as observed.
func secondaryMembersOf(source *v1beta1.InferenceService, minimums splitMinimums, secondaries map[v1beta1.ComponentType]int32, observations map[string]plannedMemberObservation) (map[v1beta1.ComponentType]map[string]secondaryMember, error) {
	candidates := source.Status.Placement.Candidates
	members := map[v1beta1.ComponentType]map[string]secondaryMember{}
	for _, candidate := range candidates {
		if !standsOnItsShare(candidate.Allocation) {
			continue
		}
		observed := observations[candidate.Cluster]
		applied := memberFloors(observed.Member)
		shares, _, err := minimums.memberShares(source, candidates, candidate, applied)
		if err != nil {
			return nil, err
		}
		next := floorMap(shares)
		for component := range secondaries {
			if members[component] == nil {
				members[component] = map[string]secondaryMember{}
			}
			members[component][candidate.Cluster] = secondaryMemberView(observed, component, applied[component], next[component])
		}
	}
	return members, nil
}

// secondaryMemberView is one member's view of a secondary component.
func secondaryMemberView(observed plannedMemberObservation, component v1beta1.ComponentType, applied, next int32) secondaryMember {
	return secondaryMember{
		Applied: applied, Next: next, Ready: observed.ComponentReady[component],
		Known: observed.MemberState.Known, Serving: observed.MemberState.Routable,
	}
}

// secondaryHoldsPerMember regroups each component's holds under the member
// that keeps them.
func secondaryHoldsPerMember(secondaries map[v1beta1.ComponentType]int32, members map[v1beta1.ComponentType]map[string]secondaryMember) map[string]map[v1beta1.ComponentType]int32 {
	out := map[string]map[v1beta1.ComponentType]int32{}
	for component, minimum := range secondaries {
		for name, held := range secondaryFloorHolds(minimum, members[component]) {
			if out[name] == nil {
				out[name] = map[v1beta1.ComponentType]int32{}
			}
			out[name][component] = held
		}
	}
	return out
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
