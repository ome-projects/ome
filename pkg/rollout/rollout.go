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
// The coordination gates read EffectiveCoordination instead, which keeps
// the recorded members and takes a group's ratio rule, a rolling group's
// budgets and the group's presence from the spec as it is now.
func Effective(isvc *v1beta1.InferenceService, policies Policies) *v1beta1.RolloutSpec {
	if isvc == nil {
		return nil
	}
	if isvc.Status.Rollout != nil && isvc.Status.Rollout.ActiveRun != nil {
		return isvc.Status.Rollout.ActiveRun.Plan.AsRolloutSpec(isvc.Spec.Rollout)
	}
	return resolveLive(isvc.Spec.Rollout, policies)
}

// EffectiveCoordination returns the rollout view the coordination gates and
// the group phase machine consume. With no run open it is the live view.
// While a run is open it is the recorded plan: every recorded group keeps
// the members it recorded, and every recorded group that is not a canary
// takes two facts from the spec group that still lists any of its members,
// as that group reads now: the ratio rule (maintainRatio, with its
// tolerance) and, when both are rollingUpdate, the budgets (maxSurge and
// maxUnavailable). A recorded group that shares no member with any spec
// group any more is handed over with no members, so it takes no part in
// coordination and the groups after it keep their names; its members roll
// on their own budgets, and the pairing gates alone keep reading it from
// the recorded plan. A changed member list is drift that applies at the
// next run: the recorded group continues with the members it names. The
// group order, each group's progression kind and every canary group stay
// as recorded until the run closes or the operator asks for the plan to be
// read again.
func EffectiveCoordination(isvc *v1beta1.InferenceService, policies Policies) *v1beta1.RolloutSpec {
	if isvc == nil {
		return nil
	}
	if isvc.Status.Rollout == nil || isvc.Status.Rollout.ActiveRun == nil {
		return resolveLive(isvc.Spec.Rollout, policies)
	}
	recorded := isvc.Status.Rollout.ActiveRun.Plan.AsRolloutSpec(isvc.Spec.Rollout)
	for i := range recorded.Groups {
		g := &recorded.Groups[i]
		if g.DeclaredProgression() == v1beta1.RolloutProgressionCanary {
			continue
		}
		live := declaredGroupSharing(isvc.Spec.Rollout, g.Components)
		if live == nil {
			g.Components = nil
			continue
		}
		g.MaintainRatio = live.MaintainRatio.DeepCopy()
		if g.RollingUpdate != nil && live.RollingUpdate != nil {
			g.RollingUpdate = live.RollingUpdate.DeepCopy()
		}
	}
	return recorded
}

// declaredGroupSharing is the first spec group that lists any of the given
// Components, or nil when the spec declares none.
func declaredGroupSharing(spec *v1beta1.RolloutSpec, components []v1beta1.ComponentType) *v1beta1.RolloutGroup {
	if spec == nil {
		return nil
	}
	for i := range spec.Groups {
		if sharesAMember(spec.Groups[i].Components, components) {
			return &spec.Groups[i]
		}
	}
	return nil
}

// sharesAMember reports whether two member lists name a Component in common.
func sharesAMember(a, b []v1beta1.ComponentType) bool {
	for _, x := range a {
		for _, y := range b {
			if x == y {
				return true
			}
		}
	}
	return false
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
// through: the member with the lowest primary rank (router, else engine,
// else decoder), else the first member. A group's canary run state is stored
// under this Component.
func PrimaryOf(g *v1beta1.RolloutGroup) v1beta1.ComponentType {
	if g == nil || len(g.Components) == 0 {
		return ""
	}
	for _, preferred := range v1beta1.RolesByPrimaryRank() {
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
