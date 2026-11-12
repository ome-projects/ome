package coordination

import (
	"context"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/rollout"
)

// GateContext caches the per-(ISVC, Component) prelude work shared by
// every Check*Gate: nil-ISVC and absent-coord-block short-circuits, the
// ResolveGroups + MembershipFor resolution, and the short-circuit
// reason string. Built once via ResolveGateContext and threaded into
// each gate.
//
// The dispatcher pays for the prelude exactly once per per-Instance
// tick instead of once per gate (4×: ratio + surge | unavailability +
// sequential). The existing Check*Gate public entry points are
// preserved as thin wrappers that build a fresh GateContext on every
// call — direct callers (and the large existing test surface) don't
// need to know about the helper.
type GateContext struct {
	// ISVC is the InferenceService the gate stack is reasoning about.
	// Nil when ResolveGateContext was called with a nil ISVC.
	ISVC *v1beta1.InferenceService

	// Component identifies the Component the gate stack is being asked
	// to allow / deny work for.
	Component v1beta1.ComponentType

	// Group is the resolved coordination group that owns Component. Zero
	// value when ShortCircuit is set.
	Group ResolvedGroup

	// ShortCircuit is true when the gate stack should bypass this
	// Component entirely (no coordination block declared, Component not
	// in any group, or the ISVC pointer was nil). Callers that observe
	// ShortCircuit=true should return (allowed=true, ShortReason)
	// without further gate work.
	ShortCircuit bool

	// ShortReason is the operator-facing explanation when ShortCircuit
	// is true. Empty otherwise.
	ShortReason string

	// Hold is the fail-closed inverse of ShortCircuit: the Component belongs
	// to a declared rollout group but no run has recorded an effective plan
	// (the run is opening this pass, or the plan is held as unresolvable), so
	// updates must be DENIED. Without it, the window between a target
	// divergence and the recorded plan — or a hold — would roll the Component forward
	// on a plan nothing validated, silently skipping its declared gates.
	Hold bool

	// HoldReason is the operator-facing explanation when Hold is true.
	HoldReason string

	// Ctx provides the context for reading authoritative IR status.
	Ctx context.Context

	// Reads is the client.Reader for fetching InferenceReplica objects.
	// A nil reader degrades to "no observation yet" (gates allow — same
	// as a missing IR); a non-nil reader whose reads FAIL makes the
	// gates fail closed (deny with a retryable reason). There is no
	// fallback to the ISVC's copied LifecycleStatus.
	Reads client.Reader

	// ComponentPods hands the unavailability gate the Component's own pods
	// for the pass, so a pass that consults the gate for several Instances
	// lists them once; nil, and the gate lists them through Reads at each
	// consult.
	ComponentPods PodSource

	// TargetRevision is the ControllerRevision the Component's Instances
	// move to in this pass: the rollback revision while a rollback is
	// recorded, else the spec target. The pairing gate reads the target
	// cohort's protocol from it, so a revert is simulated toward the stable
	// cohort even while the live spec field still names the rejected one.
	// Nil fails the pairing gate closed.
	TargetRevision *appsv1.ControllerRevision

	// Defaults are the operator-configured fill-ins the groups were
	// resolved with; the batch step reads its rules and the fallback
	// tolerance from them.
	Defaults GroupDefaults

	// Scope says which gates read Group.
	Scope GroupScope
}

// GroupScope is which gates read a GateContext's Group.
type GroupScope uint8

const (
	// CoordinationGroup is the Component's coordination group; every gate
	// reads it.
	CoordinationGroup GroupScope = iota
	// CanaryUnit is a canary group that lists both the engine and the
	// decoder, read by the batch step and the two pairing gates: the canary
	// executor stages the unit's capacity through each member's partition
	// and each member's own budget paces its starts; the operator's default
	// pacing stands in for the pacing block a canary group does not carry,
	// so the batch step paces the two sides' starts inside a step and under
	// a revert, and the pairing gates keep each side's pairable minimum
	// under its own budget during a pairing-protocol change, the way they
	// do for a blueGreen group. No other coordination gate reads the unit.
	CanaryUnit
	// RecordedGroupPairing is a rolling or blue-green group the open run
	// recorded over the engine and the decoder that shares no member with
	// any spec group any more, read by the two pairing gates alone: its
	// members roll on their own budgets, and a pairing-protocol change in
	// flight keeps the pairable minimum the run opened with until the run
	// closes.
	RecordedGroupPairing
)

// PodSource answers the Component's pods, the same set on every call within
// a pass.
type PodSource func() ([]*corev1.Pod, error)

// ResolveGateContext walks the shared prelude every Check*Gate runs:
//
//   - nil ISVC                → short-circuit with "nil ISVC"
//   - no coordination block   → short-circuit with "no coordination block"
//   - Component in no group   → short-circuit with "component not in any coord group"
//   - Component in a canary group that lists the engine and the decoder
//     → the unit's members as the group, scoped CanaryUnit, for the batch
//     step and the pairing gates
//   - Component in a recorded rolling or blue-green group over the engine
//     and the decoder that the spec no longer declares
//     → the recorded group, scoped RecordedGroupPairing, for the pairing
//     gates alone
//   - otherwise               → return the resolved group, ready for per-gate logic
//
// Returns a GateContext that subsequent CheckRatio / CheckSurge /
// CheckUnavailability / CheckSequential calls thread through. The
// dispatcher calls this once per per-Instance tick so the
// ResolveGroups + MembershipFor work doesn't run 4 times per dispatch.
//
// The gate methods read authoritative InferenceReplica status via ctx and
// reads. A nil reads degrades every read to "no observation yet" (see the
// GateContext.Reads doc); it does not fall back to isvc.Status.Components.
//
// Resolution runs without operator-configured group defaults; callers with
// access to operator configuration (the dispatcher) use
// ResolveGateContextWithDefaults so config-driven fills (e.g. the ratio
// tolerance) reach the gates.
func ResolveGateContext(ctx context.Context, reads client.Reader, isvc *v1beta1.InferenceService, component v1beta1.ComponentType) GateContext {
	return ResolveGateContextWithDefaults(ctx, reads, isvc, component, GroupDefaults{})
}

// ResolveGateContextWithDefaults is ResolveGateContext with the
// operator-configured group defaults applied during group resolution.
func ResolveGateContextWithDefaults(ctx context.Context, reads client.Reader, isvc *v1beta1.InferenceService, component v1beta1.ComponentType, defaults GroupDefaults) GateContext {
	if isvc == nil {
		return GateContext{
			Component:    component,
			ShortCircuit: true,
			ShortReason:  "nil ISVC",
			Ctx:          ctx,
			Reads:        reads,
		}
	}
	if held, reason := planHold(isvc, component); held {
		return GateContext{
			ISVC:       isvc,
			Component:  component,
			Hold:       true,
			HoldReason: reason,
			Ctx:        ctx,
			Reads:      reads,
		}
	}
	// The hold above covers every unpinned state of a grouped Component, so
	// the view read here is the recorded plan, which needs no observed
	// policies; the members are the recorded ones, while a group's ratio
	// rule, a rolling group's budgets and the group's presence come from
	// the spec as it is now, so an edit of those reaches the next start and
	// a changed member list waits for the next run.
	effective := rollout.EffectiveCoordination(isvc, rollout.Policies{})
	groups := ResolveGroups(effective, defaults)
	if group, ok := MembershipFor(groups, component); ok {
		return GateContext{
			ISVC:      isvc,
			Component: component,
			Group:     group,
			Ctx:       ctx,
			Reads:     reads,
			Defaults:  defaults,
		}
	}
	if unit, ok := canaryUnitGroup(effective, component, defaults); ok {
		return GateContext{
			ISVC:      isvc,
			Component: component,
			Group:     unit,
			Scope:     CanaryUnit,
			Ctx:       ctx,
			Reads:     reads,
			Defaults:  defaults,
		}
	}
	if group, ok := recordedPairingGroup(isvc, component, defaults); ok {
		return GateContext{
			ISVC:      isvc,
			Component: component,
			Group:     group,
			Scope:     RecordedGroupPairing,
			Ctx:       ctx,
			Reads:     reads,
			Defaults:  defaults,
		}
	}
	reason := "component not in any coord group"
	if len(groups) == 0 {
		reason = "no coordination groups"
	}
	return GateContext{
		ISVC:         isvc,
		Component:    component,
		ShortCircuit: true,
		ShortReason:  reason,
		Ctx:          ctx,
		Reads:        reads,
	}
}

// recordedPairingGroup is the rolling or blue-green group the open run
// recorded over component, resolved as recorded, when the spec no longer
// declares a group sharing a member with it and the recorded group lists
// both the engine and the decoder. A pairing-protocol change in flight
// keeps the pairable minimum the run opened with until the run closes,
// whatever the operator did to the group meanwhile.
func recordedPairingGroup(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, defaults GroupDefaults) (ResolvedGroup, bool) {
	if isvc.Status.Rollout == nil || isvc.Status.Rollout.ActiveRun == nil {
		return ResolvedGroup{}, false
	}
	recorded := ResolveGroups(rollout.Effective(isvc, rollout.Policies{}), defaults)
	group, ok := MembershipFor(recorded, component)
	if !ok || !groupPairsEngineAndDecoder(group) {
		return ResolvedGroup{}, false
	}
	return group, true
}

// planHold is the fail-closed plan gate: a Component in ANY declared rollout
// group (raw spec membership — canary and ref-only groups included, which the
// coordination-only ResolveGroups deliberately excludes) may not take updates
// until a run records the effective plan. This is the load-bearing ordering
// invariant of the run model: between a target divergence and the recorded plan
// (different loops), and while a plan is held as unresolvable, the gate holds
// — otherwise a ref-only group would roll forward as if it were a bare
// blueGreen, skipping its declared gates.
func planHold(isvc *v1beta1.InferenceService, component v1beta1.ComponentType) (bool, string) {
	if isvc.Spec.Rollout != nil && specGroupMember(isvc.Spec.Rollout, component) &&
		(isvc.Status.Rollout == nil || isvc.Status.Rollout.ActiveRun == nil) {
		return true, "rollout plan not pinned: no active run for this rollout group (run opening, or parked on an unresolvable plan)"
	}
	return false, ""
}

// specGroupMember reports raw spec-group membership: whether the Component is
// listed in any spec.rollout.groups[] entry, independent of progression kind
// or resolvability. The plan gate keys on this, not on ResolvedGroups, so
// canary members and ref-only groups are covered too.
func specGroupMember(spec *v1beta1.RolloutSpec, component v1beta1.ComponentType) bool {
	for i := range spec.Groups {
		for _, c := range spec.Groups[i].Components {
			if c == component {
				return true
			}
		}
	}
	return false
}
