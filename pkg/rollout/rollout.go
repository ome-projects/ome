// Package rollout holds the behavior an executor needs on top of the rollout
// API types: which rollout view is in force, which group drives a Component,
// and where a canary run's state lives. The types themselves stay in
// pkg/apis/ome/v1beta1, which is a schema, not a place for executor policy.
package rollout

import "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"

// Effective returns the rollout view executors must consume: the pinned
// active-run plan while a run is open, the live spec otherwise. PairingProtocol
// is always read live — it is per-service wire-contract state and an operator
// input, not plan content.
func Effective(isvc *v1beta1.InferenceService) *v1beta1.RolloutSpec {
	if isvc == nil {
		return nil
	}
	if isvc.Status.Rollout != nil && isvc.Status.Rollout.ActiveRun != nil {
		return isvc.Status.Rollout.ActiveRun.Plan.AsRolloutSpec(isvc.Spec.Rollout)
	}
	return isvc.Spec.Rollout
}

// CanaryGroups returns every effective rollout group whose progression is
// canary, in plan order. A group owns one canary unit — the router, or
// engine+decoder — and admission rejects two groups sharing a unit, so the
// returned groups drive disjoint Components and their step machines cannot
// contend.
func CanaryGroups(isvc *v1beta1.InferenceService) []*v1beta1.RolloutGroup {
	spec := Effective(isvc)
	if spec == nil {
		return nil
	}
	var out []*v1beta1.RolloutGroup
	for i := range spec.Groups {
		if spec.Groups[i].Canary != nil {
			out = append(out, &spec.Groups[i])
		}
	}
	return out
}

// CanaryGroupFor returns the effective canary group that drives the given
// Component, or nil. Anything that resolves a plan, a step or a partition for a
// Component must use this rather than the first canary group: with a group per
// unit, the first group is another unit's ladder, and executing it would step
// one unit's capacity and traffic to another unit's weights.
func CanaryGroupFor(isvc *v1beta1.InferenceService, component v1beta1.ComponentType) *v1beta1.RolloutGroup {
	for _, g := range CanaryGroups(isvc) {
		for _, c := range g.Components {
			if CanaryUnit(c) == CanaryUnit(component) {
				return g
			}
		}
	}
	return nil
}

// CanaryGroup returns the first effective rollout group whose progression is
// canary, or nil. The pinned-plan-aware analogue of
// InferenceServiceSpec.GetCanaryGroup; executors must use this so a mid-run
// spec edit cannot change the plan under the persisted step counter.
func CanaryGroup(isvc *v1beta1.InferenceService) *v1beta1.RolloutGroup {
	spec := Effective(isvc)
	if spec == nil {
		return nil
	}
	for i := range spec.Groups {
		if spec.Groups[i].Canary != nil {
			return &spec.Groups[i]
		}
	}
	return nil
}
