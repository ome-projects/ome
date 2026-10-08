// Package rollout holds the behavior an executor needs on top of the rollout
// API types: which rollout view is in force, which group drives a Component,
// and where a canary run's state lives. The types themselves stay in
// pkg/apis/ome/v1beta1, which is a schema, not a place for executor policy.
package rollout

import "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"

// Effective returns the rollout view executors must consume: the pinned
// active-run plan while a run is open, otherwise the live spec with every
// policy reference resolved through policies. The pin is what the same
// resolver produced at run open, so a group's body reads the same in every
// phase of a run's life. PairingProtocol is always read live — it is
// per-service wire-contract state and an operator input, not plan content.
func Effective(isvc *v1beta1.InferenceService, policies Policies) *v1beta1.RolloutSpec {
	if isvc == nil {
		return nil
	}
	if isvc.Status.Rollout != nil && isvc.Status.Rollout.ActiveRun != nil {
		return isvc.Status.Rollout.ActiveRun.Plan.AsRolloutSpec(isvc.Spec.Rollout)
	}
	return resolveLive(isvc.Spec.Rollout, policies)
}

// CanaryGroups returns every effective rollout group carrying an EXECUTABLE
// canary body, in plan order. A group owns one canary unit — the router, or
// engine+decoder — and admission rejects two groups sharing a unit, so the
// returned groups drive disjoint Components and their step machines cannot
// contend.
//
// The inline body is the test on purpose: this answers "which ladder can be
// stepped", not "which group declared canary". A group whose reference does
// not resolve has no ladder, so it is absent here and its Components take
// the plan-gate hold instead. Consumers asking the declared question —
// whether a Component is gated at all — must use RolloutGroup.DeclaredProgression.
func CanaryGroups(isvc *v1beta1.InferenceService, policies Policies) []*v1beta1.RolloutGroup {
	spec := Effective(isvc, policies)
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
func CanaryGroupFor(isvc *v1beta1.InferenceService, policies Policies, component v1beta1.ComponentType) *v1beta1.RolloutGroup {
	for _, g := range CanaryGroups(isvc, policies) {
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
func CanaryGroup(isvc *v1beta1.InferenceService, policies Policies) *v1beta1.RolloutGroup {
	spec := Effective(isvc, policies)
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

// PrimaryOf is the Component a group's step machine and traffic weight run
// through: router, else engine, else decoder among the group's members, else
// the first member. A group's canary run state is stored under this Component.
func PrimaryOf(g *v1beta1.RolloutGroup) v1beta1.ComponentType {
	if g == nil || len(g.Components) == 0 {
		return ""
	}
	for _, preferred := range []v1beta1.ComponentType{v1beta1.RouterComponent, v1beta1.EngineComponent, v1beta1.DecoderComponent} {
		for _, c := range g.Components {
			if c == preferred {
				return c
			}
		}
	}
	return g.Components[0]
}

// GroupCanaryStatusFor returns the run state of the canary group that drives
// component, or nil when no canary group covers it. The state is keyed by the
// group's primary, which is not the member's own unit when one group covers
// both units: a P/D pair rolled through a router-primary group reads the
// router's step, and reading its own unit would find nothing and stay at the
// first step's capacity while the primary advances.
func GroupCanaryStatusFor(isvc *v1beta1.InferenceService, policies Policies, component v1beta1.ComponentType) *v1beta1.CanaryStatus {
	g := CanaryGroupFor(isvc, policies, component)
	if g == nil {
		return nil
	}
	return CanaryStatusFor(&isvc.Status, PrimaryOf(g))
}

// CanaryOwnedComponents returns every Component a canary group governs: the
// members of each group the spec declares as canary (inline or by policyRef)
// and of each canary group the pinned plan carries. Ownership follows the
// declaration, so it needs no resolution. The canary executor is the sole
// producer of their per-revision Services, traffic targets and rolled-out
// revision fields, the primary's and every secondary's alike; coordination's
// pod-proportional writer leaves them alone, so a canary weight is never
// overwritten and a group edited out of the spec mid-run stays with the run
// that pins it.
func CanaryOwnedComponents(isvc *v1beta1.InferenceService) map[v1beta1.ComponentType]struct{} {
	owned := map[v1beta1.ComponentType]struct{}{}
	if isvc == nil {
		return owned
	}
	for _, g := range isvc.Spec.GetRolloutGroups() {
		if g.DeclaredProgression() != v1beta1.RolloutProgressionCanary {
			continue
		}
		for _, c := range g.Components {
			owned[c] = struct{}{}
		}
	}
	if isvc.Status.Rollout == nil || isvc.Status.Rollout.ActiveRun == nil {
		return owned
	}
	for _, pinned := range isvc.Status.Rollout.ActiveRun.Plan.Groups {
		if pinned.Group.Canary == nil {
			continue
		}
		for _, c := range pinned.Group.Components {
			owned[c] = struct{}{}
		}
	}
	return owned
}
