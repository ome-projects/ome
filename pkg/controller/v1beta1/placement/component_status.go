package placement

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"knative.dev/pkg/apis"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/placement/affinity"
)

// componentOrder lists the scaled components in the order status reports them.
var componentOrder = []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent}

// splitDesired returns the accepted plan's desired primary targets and the
// allocations its weights are read from.
func splitDesired(source *v1beta1.InferenceService) (map[string]int32, map[string]v1beta1.CandidateAllocationStatus) {
	targets := map[string]int32{}
	allocations := map[string]v1beta1.CandidateAllocationStatus{}
	for _, candidate := range source.Status.Placement.Candidates {
		if candidate.Allocation == nil || candidate.Cluster == "" {
			continue
		}
		targets[candidate.Cluster] = candidate.Allocation.DesiredReplicas
		allocations[candidate.Cluster] = *candidate.Allocation
	}
	return targets, allocations
}

// componentFloorsFor reports each Split member's floors per component: the floor
// on its member copy now and the floor at the accepted plan's desired shares.
// A source with only its primary component reports none.
func componentFloorsFor(source *v1beta1.InferenceService, observations map[string]plannedMemberObservation) map[string][]v1beta1.CandidateComponentFloor {
	mode := placementMode(source)
	if (mode != v1beta1.PlacementModeSplit && mode != v1beta1.PlacementModeSplitByCapacity) || source.Status.Placement == nil {
		return nil
	}
	minimums, err := splitComponentMinimums(source)
	if err != nil || len(minimums.secondaries()) == 0 {
		return nil
	}
	targets, allocations := splitDesired(source)
	desired, err := apportionSplitFloors(minimums, targets, splitWeightsOf(source, allocations))
	if err != nil {
		return nil
	}
	out := map[string][]v1beta1.CandidateComponentFloor{}
	for name := range targets {
		current := memberFloors(observations[name].Member)
		want := floorMap(desired[name])
		var floors []v1beta1.CandidateComponentFloor
		placed := false
		for _, component := range componentOrder {
			if _, scaled := minimums.floors[component]; !scaled {
				continue
			}
			floor := v1beta1.CandidateComponentFloor{Component: component, Current: current[component], Desired: want[component]}
			placed = placed || floor.Current > 0 || floor.Desired > 0
			floors = append(floors, floor)
		}
		if placed {
			out[name] = floors
		}
	}
	return out
}

// componentSharesCondition reports how a Split source whose terms set weights
// splits each component, and which weights go unused because their cluster
// holds no primary component. Every other source clears it.
func componentSharesCondition(source *v1beta1.InferenceService) policyCondition {
	condition := policyCondition{condType: apis.ConditionType(v1beta1.PlacementComponentShares), clear: true}
	if placementMode(source) != v1beta1.PlacementModeSplit || !affinity.UsesRatios(source.Spec.Placement.ClusterAffinity) || source.Status.Placement == nil {
		return condition
	}
	minimums, err := splitComponentMinimums(source)
	if err != nil {
		return condition
	}
	targets, allocations := splitDesired(source)
	weights := splitWeightsOf(source, allocations)
	own := weights.ownWeights()
	reason := "FollowEngineShares"
	var parts, unused []string
	for _, component := range componentOrder {
		if _, secondary := minimums.secondaries()[component]; !secondary {
			continue
		}
		if _, independent := own[component]; independent {
			parts = append(parts, fmt.Sprintf("%s is split by its own weights", component))
			reason = "OwnWeights"
		} else {
			parts = append(parts, fmt.Sprintf("%s follows the %s shares", component, minimums.primary))
		}
	}
	for _, component := range componentOrder {
		byCluster := weights.components[component]
		if component == minimums.primary {
			byCluster = weights.primary
		}
		for _, name := range slices.Sorted(maps.Keys(byCluster)) {
			if byCluster[name] > 0 && targets[name] == 0 {
				unused = append(unused, fmt.Sprintf("%s weight on %s", component, name))
			}
		}
	}
	if len(unused) > 0 {
		parts = append(parts, fmt.Sprintf("unused because the cluster holds no %s: %s", minimums.primary, strings.Join(unused, ", ")))
	}
	if len(parts) == 0 {
		return condition
	}
	condition.clear = false
	condition.cond = apis.Condition{Status: corev1.ConditionTrue, Reason: reason, Message: strings.Join(parts, "; ")}
	return condition
}
