package coordination

import (
	"context"

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
	// to a declared rollout group but no run has pinned an effective plan
	// (the run is opening this pass, or the plan is parked unresolvable), so
	// updates must be DENIED. Without it, the window between a target
	// divergence and the pin — or a park — would roll the Component forward
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
}

// ResolveGateContext walks the shared prelude every Check*Gate runs:
//
//   - nil ISVC                → short-circuit with "nil ISVC"
//   - no coordination block   → short-circuit with "no coordination block"
//   - Component in no group   → short-circuit with "component not in any coord group"
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
	// Fail-closed plan gate: a Component in ANY declared rollout group (raw
	// spec membership — canary and ref-only groups included, which the
	// coordination-only ResolveGroups below deliberately excludes) may not
	// take updates until a run pins the effective plan. This is the load-
	// bearing ordering invariant of the run model: between a target
	// divergence and the pin (different loops), and while a plan is parked
	// unresolvable, the gate holds — otherwise a ref-only group would roll
	// forward as if it were a bare blueGreen, skipping its declared gates.
	if isvc.Spec.Rollout != nil && specGroupMember(isvc.Spec.Rollout, component) &&
		(isvc.Status.Rollout == nil || isvc.Status.Rollout.ActiveRun == nil) {
		return GateContext{
			ISVC:       isvc,
			Component:  component,
			Hold:       true,
			HoldReason: "rollout plan not pinned: no active run for this rollout group (run opening, or parked on an unresolvable plan)",
			Ctx:        ctx,
			Reads:      reads,
		}
	}
	groups := ResolveGroups(rollout.Effective(isvc), defaults)
	if len(groups) == 0 {
		return GateContext{
			ISVC:         isvc,
			Component:    component,
			ShortCircuit: true,
			ShortReason:  "no coordination groups",
			Ctx:          ctx,
			Reads:        reads,
		}
	}
	group, ok := MembershipFor(groups, component)
	if !ok {
		return GateContext{
			ISVC:         isvc,
			Component:    component,
			ShortCircuit: true,
			ShortReason:  "component not in any coord group",
			Ctx:          ctx,
			Reads:        reads,
		}
	}
	return GateContext{
		ISVC:      isvc,
		Component: component,
		Group:     group,
		Ctx:       ctx,
		Reads:     reads,
	}
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
