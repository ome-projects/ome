// Cross-Component rollout coordination validation (v2 spec.rollout.groups[]).
//
// Webhook-level admission rules for the coordination-style progressions
// (blueGreen / rollingUpdate) on spec.rollout.groups[]: per-group structural
// checks (valid components, order subset), cross-group checks (a Component
// appears in at most one group; every group member is a declared Component;
// groups are OMENative), pacing zero-budget, and the RollingUpdate lockstep on
// update.
// The one-of progression shape and order⊆components are also enforced by the CRD
// CEL rules; these are the value-level mirrors plus the cross-group rules CEL
// can't express. Canary groups are validated by ValidateCanary.
package validation

import (
	"fmt"
	"strings"

	apiequality "k8s.io/apimachinery/pkg/api/equality"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// Admission rejection reasons. Operators and tests reference these by
// name, so they're exported constants.
const (
	ReasonDuplicateComponentInCoordinationGroups = "DuplicateComponentInCoordinationGroups"
	ReasonInvalidComponentInCoordinationGroup    = "InvalidComponentInCoordinationGroup"
	ReasonCoordinationRequiresOMENative          = "CoordinationRequiresOMENative"
	ReasonRatioToleranceTooHigh                  = "RatioToleranceTooHigh"
	ReasonRatioToleranceOutOfRange               = "RatioToleranceOutOfRange"
	ReasonOrphanCoordinationGroup                = "OrphanCoordinationGroup"
	ReasonInvalidCoordinationOrder               = "InvalidCoordinationOrder"
	ReasonRollingUpdateLockstepViolation         = "RollingUpdateLockstepViolation"
	ReasonPairingProtocolChangeUncoordinated     = "PairingProtocolChangeUncoordinated"
	// ReasonSequentialDuplicateOrder flags a duplicate entry in a group's Order
	// (the rule applies to any group's surge Order, not only the v1 Sequential
	// policy that is gone).
	ReasonSequentialDuplicateOrder = "SequentialDuplicateOrder"
	// ReasonSoakNotHonored flags a Soak the engine would silently drop. Soak is
	// honored only by the Sequential state machine (a collapsed run of
	// single-Component blueGreen groups); it is never honored on a canary,
	// multi-Component, or rollingUpdate group, nor on a rollout that does not
	// collapse to Sequential.
	ReasonSoakNotHonored = "SoakNotHonored"
	// ReasonGroupOrderingNotHonored flags a multi-group rollout whose list
	// order the engine would silently ignore. groups[] promises group N
	// completes before group N+1 begins, but the engine enforces that only
	// for a run of single-Component blueGreen groups; any other multi-group
	// list runs concurrently on its disjoint Components.
	ReasonGroupOrderingNotHonored = "GroupOrderingNotHonored"
	// ReasonOrderNotHonored flags a non-empty groups[i].order. No current
	// progression applies Order as a surge sequence — the Components in a
	// group advance together — so accepting it would promise a sequence
	// that never happens.
	ReasonOrderNotHonored = "OrderNotHonored"
)

// ratioToleranceWarningThreshold is the upper bound the validator considers
// operator-safe for the MaintainRatio tolerance knob. Values above this trigger a
// non-blocking warning since they effectively disable ratio enforcement.
const ratioToleranceWarningThreshold int32 = 50

// validComponents is the closed set of Components allowed in a rollout group.
var validComponents = map[v1beta1.ComponentType]struct{}{
	v1beta1.RouterComponent:  {},
	v1beta1.EngineComponent:  {},
	v1beta1.DecoderComponent: {},
}

// ValidateCoordination runs the coordination-style (blueGreen / rollingUpdate)
// admission rules over spec.rollout.groups[]. Returns the first error; nil on
// success. Update-time (RollingUpdate lockstep) checks live in
// ValidateCoordinationUpdate.
func ValidateCoordination(spec *v1beta1.InferenceServiceSpec) error {
	groups := spec.GetRolloutGroups()
	if len(groups) == 0 {
		return nil
	}
	if err := validateGroupComponents(groups); err != nil {
		return err
	}
	if err := validateNoDuplicateMembership(groups); err != nil {
		return err
	}
	if err := validateOrphanGroups(spec, groups); err != nil {
		return err
	}
	if err := validateComponentsAreOMENative(spec, groups); err != nil {
		return err
	}
	if err := validateGroupRollingUpdateBudgets(groups); err != nil {
		return err
	}
	if err := validateRatioToleranceRange(groups); err != nil {
		return err
	}
	if err := validatePacingNotZeroBudget(groups); err != nil {
		return err
	}
	if err := validateSoakOnlyWhenSequenced(groups); err != nil {
		return err
	}
	return nil
}

// ValidateCoordinationUpdate runs the update-time rule comparing old vs new spec:
// a rollingUpdate group bumps all its Components together (lockstep). Structural
// validation runs separately on the new spec.
func ValidateCoordinationUpdate(oldSpec, newSpec *v1beta1.InferenceServiceSpec) error {
	groups := newSpec.GetRolloutGroups()
	for i := range groups {
		g := &groups[i]
		// Kind-resolved so a group whose rollingUpdate comes from a declared
		// policyRef obeys the same all-together contract as an inline one.
		if rolloutGroupKind(g) != v1beta1.RolloutProgressionRollingUpdate {
			continue
		}
		if err := validateRollingUpdateLockstep(oldSpec, newSpec, g.Components); err != nil {
			return err
		}
	}
	return nil
}

// ValidatePairingProtocolUpdate rejects a pairing-protocol change between two
// non-empty values unless engine and decoder roll as ONE group under blueGreen
// or canary. Any other shape — independent Components, separate groups, or a
// rollingUpdate group — reaches a step where every serving engine speaks a
// protocol no serving decoder does (or vice versa), leaving routing with no
// valid pair. Transitions to or from the empty value are always admitted:
// empty pairs with anything, so every intermediate mix stays routable. So is
// a change on a spec without both engine and decoder — there is no pair to
// break.
func ValidatePairingProtocolUpdate(oldSpec, newSpec *v1beta1.InferenceServiceSpec) error {
	oldProtocol := oldSpec.RolloutPairingProtocol()
	newProtocol := newSpec.RolloutPairingProtocol()
	if oldProtocol == newProtocol || oldProtocol == "" || newProtocol == "" {
		return nil
	}
	if newSpec.Engine == nil || newSpec.Decoder == nil {
		return nil
	}
	for _, g := range newSpec.GetRolloutGroups() {
		// Kind-resolved: a declared blueGreen/canary policyRef keeps the pair
		// atomic exactly like the inline arm would.
		if kind := rolloutGroupKind(&g); kind != v1beta1.RolloutProgressionBlueGreen && kind != v1beta1.RolloutProgressionCanary {
			continue
		}
		hasEngine, hasDecoder := false, false
		for _, c := range g.Components {
			switch c {
			case v1beta1.EngineComponent:
				hasEngine = true
			case v1beta1.DecoderComponent:
				hasDecoder = true
			}
		}
		if hasEngine && hasDecoder {
			return nil
		}
	}
	return fmt.Errorf("spec.rollout.pairingProtocol: changing the pairing protocol (%q -> %q) requires engine and decoder to roll as one blueGreen or canary group; declare a spec.rollout group listing both components (rollingUpdate groups and independent rollouts cannot keep a pairable engine+decoder pair serving through the transition) (%s)",
		oldProtocol, newProtocol, ReasonPairingProtocolChangeUncoordinated)
}

// CoordinationRatioToleranceWarning returns a warning when any group sets a
// MaintainRatio tolerance above the safe threshold. Empty when none applies.
func CoordinationRatioToleranceWarning(spec *v1beta1.InferenceServiceSpec) string {
	groups := spec.GetRolloutGroups()
	for i := range groups {
		mr := groups[i].MaintainRatio
		if mr == nil || mr.Tolerance == nil {
			continue
		}
		if *mr.Tolerance > ratioToleranceWarningThreshold {
			return fmt.Sprintf(
				"spec.rollout.groups[%d].maintainRatio.tolerance=%d effectively disables ratio enforcement (>%d) (%s)",
				i, *mr.Tolerance, ratioToleranceWarningThreshold, ReasonRatioToleranceTooHigh)
		}
	}
	return ""
}

// validateGroupComponents checks that every coordination-style group references
// only valid Components and that its surge Order is a duplicate-free subset of
// its Components. Groups whose kind is canary (inline or ref-declared) are
// validated by ValidateCanary.
func validateGroupComponents(groups []v1beta1.RolloutGroup) error {
	for i := range groups {
		g := &groups[i]
		if rolloutGroupKind(g) == v1beta1.RolloutProgressionCanary {
			continue
		}
		for _, c := range g.Components {
			if _, ok := validComponents[c]; !ok {
				return fmt.Errorf("spec.rollout.groups[%d]: component %q is not one of router/engine/decoder (%s)",
					i, c, ReasonInvalidComponentInCoordinationGroup)
			}
		}
		if err := validateOrderSubset(i, g.Components, g.Order); err != nil {
			return err
		}
	}
	return nil
}

// validateOrderSubset enforces that order is a duplicate-free subset of
// components. Applies to any group with an Order (the surge sequence).
func validateOrderSubset(idx int, components, order []v1beta1.ComponentType) error {
	if len(order) == 0 {
		return nil
	}
	in := make(map[v1beta1.ComponentType]struct{}, len(components))
	for _, c := range components {
		in[c] = struct{}{}
	}
	seen := make(map[v1beta1.ComponentType]struct{}, len(order))
	for _, c := range order {
		if _, ok := in[c]; !ok {
			return fmt.Errorf("spec.rollout.groups[%d]: order entry %q is not in components (%s)",
				idx, c, ReasonInvalidCoordinationOrder)
		}
		if _, dup := seen[c]; dup {
			return fmt.Errorf("spec.rollout.groups[%d]: order entry %q appears more than once (%s)",
				idx, c, ReasonSequentialDuplicateOrder)
		}
		seen[c] = struct{}{}
	}
	return nil
}

// validateNoDuplicateMembership rejects a Component appearing in more than one
// group (across ALL groups, canary included). Each Component rolls under exactly
// one group.
func validateNoDuplicateMembership(groups []v1beta1.RolloutGroup) error {
	seen := map[v1beta1.ComponentType]int{}
	for i := range groups {
		for _, c := range groups[i].Components {
			if prev, ok := seen[c]; ok {
				return fmt.Errorf("spec.rollout.groups: component %q appears in groups[%d] and groups[%d] (%s)",
					c, prev, i, ReasonDuplicateComponentInCoordinationGroups)
			}
			seen[c] = i
		}
	}
	return nil
}

// validateOrphanGroups requires EVERY Component named by a group to be declared
// on the InferenceService. A group member without a declared Component can never
// produce a live replica, so the group's rollout could never complete — the
// error names the group index and each missing member so the operator can fix
// the exact gap.
func validateOrphanGroups(spec *v1beta1.InferenceServiceSpec, groups []v1beta1.RolloutGroup) error {
	declared := declaredComponents(spec)
	for i := range groups {
		var missing []string
		for _, c := range groups[i].Components {
			if _, ok := declared[c]; !ok {
				missing = append(missing, fmt.Sprintf("%q", c))
			}
		}
		if len(missing) > 0 {
			return fmt.Errorf("spec.rollout.groups[%d]: component(s) %s not declared on the InferenceService (%s)",
				i, strings.Join(missing, ", "), ReasonOrphanCoordinationGroup)
		}
	}
	return nil
}

// validateComponentsAreOMENative requires every Component in a coordination-style
// group to declare OMENative. Canary-kind groups (inline or ref-declared) are
// checked in ValidateCanary. An undeclared member resolves to an empty mode and
// fails here too — the orphan check runs first, so its more specific error
// normally wins.
func validateComponentsAreOMENative(spec *v1beta1.InferenceServiceSpec, groups []v1beta1.RolloutGroup) error {
	modeFor := omenativeDeploymentModeMap(spec)
	for i := range groups {
		g := &groups[i]
		if rolloutGroupKind(g) == v1beta1.RolloutProgressionCanary {
			continue
		}
		for _, c := range g.Components {
			if declared := modeFor[c]; declared != string(constants.OMENative) {
				return fmt.Errorf("spec.rollout.groups[%d]: component %q has deploymentMode=%q; coordination requires OMENative (%s)",
					i, c, declared, ReasonCoordinationRequiresOMENative)
			}
		}
	}
	return nil
}

// validateGroupRollingUpdateBudgets runs the semantic IntOrString checks
// (integers >= 0, percent strings within 0%-100%) over every rollingUpdate
// group's maxSurge/maxUnavailable, reporting the exact field path. The runtime
// resolver turns malformed or negative values into a zero budget, which would
// silently stall the roll — admission rejects them instead.
func validateGroupRollingUpdateBudgets(groups []v1beta1.RolloutGroup) error {
	for i := range groups {
		ru := groups[i].RollingUpdate
		if ru == nil {
			continue
		}
		base := fmt.Sprintf("spec.rollout.groups[%d].rollingUpdate.", i)
		if err := validateBudgetIntOrString(base+"maxSurge", ru.MaxSurge); err != nil {
			return err
		}
		if err := validateBudgetIntOrString(base+"maxUnavailable", ru.MaxUnavailable); err != nil {
			return err
		}
	}
	return nil
}

// validateRatioToleranceRange is the value-level mirror of the CRD's
// Minimum=0/Maximum=100 bounds on maintainRatio.tolerance, for callers that
// bypass schema validation. Nil (omitted) is valid — the operator-configured
// default applies at resolution.
func validateRatioToleranceRange(groups []v1beta1.RolloutGroup) error {
	for i := range groups {
		mr := groups[i].MaintainRatio
		if mr == nil || mr.Tolerance == nil {
			continue
		}
		if *mr.Tolerance < 0 || *mr.Tolerance > 100 {
			return fmt.Errorf("spec.rollout.groups[%d]: maintainRatio.tolerance=%d must be between 0 and 100 (%s)",
				i, *mr.Tolerance, ReasonRatioToleranceOutOfRange)
		}
	}
	return nil
}

// validatePacingNotZeroBudget rejects a rollingUpdate group whose maxSurge AND
// maxUnavailable both RESOLVE to zero budget at runtime: no surge headroom and
// no drain headroom deadlocks the roll. Resolved semantics (not just literal
// zeros) keep the rule correct on its own; in the ValidateCoordination flow the
// per-field semantic checks run first, so the operator sees the more precise
// error for malformed or negative forms. nil (defaulted) values resolve to 25%
// and are not zero.
func validatePacingNotZeroBudget(groups []v1beta1.RolloutGroup) error {
	for i := range groups {
		ru := groups[i].RollingUpdate
		if ru == nil {
			continue
		}
		if budgetResolvesToZero(ru.MaxSurge) && budgetResolvesToZero(ru.MaxUnavailable) {
			return fmt.Errorf("spec.rollout.groups[%d].rollingUpdate: maxSurge and maxUnavailable both resolve to zero, which deadlocks the rollout — set at least one non-zero (%s)",
				i, ReasonZeroBudgetPacingUnstartable)
		}
	}
	return nil
}

// validateRollingUpdateLockstep enforces the all-together contract of a
// rollingUpdate group on update: if any grouped Component's revision-affecting
// spec changed, every grouped Component's must have changed. The compare
// covers the full revision-affecting view (componentRevisionSpecChanged), not
// just container images — an env, command, resource, volume, label, or
// annotation change re-renders the pod template and rolls the Component
// exactly like an image bump, so it must obey the same group contract.
func validateRollingUpdateLockstep(oldSpec, newSpec *v1beta1.InferenceServiceSpec, components []v1beta1.ComponentType) error {
	var changed, unchanged []v1beta1.ComponentType
	for _, c := range components {
		if componentRevisionSpecChanged(oldSpec, newSpec, c) {
			changed = append(changed, c)
		} else {
			unchanged = append(unchanged, c)
		}
	}
	if len(changed) > 0 && len(unchanged) > 0 {
		return fmt.Errorf("spec.rollout: rollingUpdate group requires all components to bump together; changed %v but not %v (%s)",
			changed, unchanged, ReasonRollingUpdateLockstepViolation)
	}
	return nil
}

// declaredComponents returns the set of Components present on the spec.
func declaredComponents(spec *v1beta1.InferenceServiceSpec) map[v1beta1.ComponentType]struct{} {
	out := map[v1beta1.ComponentType]struct{}{}
	if spec == nil {
		return out
	}
	if spec.Engine != nil {
		out[v1beta1.EngineComponent] = struct{}{}
	}
	if spec.Decoder != nil {
		out[v1beta1.DecoderComponent] = struct{}{}
	}
	if spec.Router != nil {
		out[v1beta1.RouterComponent] = struct{}{}
	}
	return out
}

// omenativeDeploymentModeMap returns Component → effective
// deploymentMode value for each Component on the spec. Effective value
// follows the resolution chain: per-Component annotation >
// spec.deploymentMode > "" (empty when neither is set). The shape-derived
// modes (MultiNode, PDDisaggregated) are intentionally NOT considered
// — coordination only cares about the dispatch backend, not the shape.
func omenativeDeploymentModeMap(spec *v1beta1.InferenceServiceSpec) map[v1beta1.ComponentType]string {
	out := map[v1beta1.ComponentType]string{}
	if spec == nil {
		return out
	}
	if spec.Engine != nil {
		out[v1beta1.EngineComponent] = resolveComponentDeploymentMode(spec.Engine.Annotations, spec.DeploymentMode)
	}
	if spec.Decoder != nil {
		out[v1beta1.DecoderComponent] = resolveComponentDeploymentMode(spec.Decoder.Annotations, spec.DeploymentMode)
	}
	if spec.Router != nil {
		out[v1beta1.RouterComponent] = resolveComponentDeploymentMode(spec.Router.Annotations, spec.DeploymentMode)
	}
	return out
}

// rolloutCollapsesToSequential mirrors the engine's collapseSequential: the
// coordination-style (non-canary) groups fold into the Sequential state machine
// — the only path that honors Soak — exactly when there are 2+ of them and every
// one is a single-Component blueGreen group (no rollingUpdate). Anything else (a
// multi-Component group, a rollingUpdate group, or fewer than 2) runs the groups
// concurrently, where Soak is dropped. Kinds are resolved through
// rolloutGroupKind, so a declared policyRef progression shapes the collapse
// exactly like its inline equivalent.
func rolloutCollapsesToSequential(groups []v1beta1.RolloutGroup) bool {
	n := 0
	for i := range groups {
		g := &groups[i]
		kind := rolloutGroupKind(g)
		if kind == v1beta1.RolloutProgressionCanary {
			continue // canary groups are excluded from the coordination collapse
		}
		n++
		if len(g.Components) != 1 || kind != v1beta1.RolloutProgressionBlueGreen {
			return false
		}
	}
	return n >= 2
}

// validateSoakOnlyWhenSequenced rejects a Soak the engine would silently drop.
// Soak is honored only by the Sequential state machine (the collapsed run of
// single-Component blueGreen groups). It is never honored on a canary group, and
// on a rollout that does not collapse the groups run concurrently and the soak is
// dropped. Accepting it there would make an operator believe a wait is enforced
// when it is not, so admission rejects it instead.
func validateSoakOnlyWhenSequenced(groups []v1beta1.RolloutGroup) error {
	collapses := rolloutCollapsesToSequential(groups)
	for i := range groups {
		g := &groups[i]
		if g.Soak == nil {
			continue
		}
		if rolloutGroupKind(g) == v1beta1.RolloutProgressionCanary {
			return fmt.Errorf("spec.rollout.groups[%d]: soak is not honored on a canary group (the canary engine does not gate on it) (%s)",
				i, ReasonSoakNotHonored)
		}
		if !collapses {
			return fmt.Errorf("spec.rollout.groups[%d]: soak is honored only when the rollout is a sequence of single-Component groups (which roll one-at-a-time); this rollout runs groups concurrently, so the soak would be dropped (%s)",
				i, ReasonSoakNotHonored)
		}
	}
	return nil
}

// ValidateRolloutOrderingEnforced rejects rollout shapes that promise an
// ordering the engine does not enforce, mirroring the Soak rule: an ordering
// the engine would silently drop is rejected rather than accepted. Two rules:
//
//   - A non-empty groups[i].order is rejected on every group — no current
//     progression applies Order as a surge sequence.
//   - Under groupOrdering: Sequential (the default), a multi-group list is
//     accepted only when it is a pure run of single-Component blueGreen
//     groups (the shape that collapses to the Sequential state machine, the
//     one path that enforces cross-group order). Any other multi-group list —
//     a rollingUpdate, canary, or multi-Component group in the mix — runs
//     concurrently. Under groupOrdering: Concurrent the caller has declared
//     the groups independent, so there is no ordering promise left to drop
//     and the shape rule does not apply.
//
// The order rule is unconditional: Order is not applied by any progression,
// so it is dead configuration under either ordering.
//
// Runs on create; updates apply it through the
// ValidateRolloutOrderingEnforcedUpdate ratchet.
func ValidateRolloutOrderingEnforced(spec *v1beta1.InferenceServiceSpec) error {
	groups := spec.GetRolloutGroups()
	if len(groups) == 0 {
		return nil
	}
	for i := range groups {
		if len(groups[i].Order) > 0 {
			return fmt.Errorf("spec.rollout.groups[%d]: order is not applied by any progression — the Components in a group advance together; to roll Components one at a time, declare one single-Component blueGreen group per Component, in the desired sequence (%s)",
				i, ReasonOrderNotHonored)
		}
	}
	if len(groups) < 2 {
		return nil // a single group makes no cross-group ordering promise
	}
	if spec.GetRolloutGroupOrdering() == v1beta1.RolloutGroupOrderingConcurrent {
		return nil // no promise was made, so none can be dropped
	}
	for i := range groups {
		g := &groups[i]
		kind := rolloutGroupKind(g)
		var shape string
		switch {
		case kind == v1beta1.RolloutProgressionCanary:
			shape = "a canary group"
		case kind == v1beta1.RolloutProgressionRollingUpdate:
			shape = "a rollingUpdate group"
		case len(g.Components) != 1:
			shape = "a multi-Component group"
		default:
			continue
		}
		return fmt.Errorf("spec.rollout.groups: groups[] is ordered (group N completes before group N+1 begins), but that is enforced only for a run of single-Component blueGreen groups; groups[%d] is %s, so the groups would run concurrently and the declared order would be dropped — set spec.rollout.groupOrdering: Concurrent if the groups are independent and running them at the same time is what you want (%s)",
			i, shape, ReasonGroupOrderingNotHonored)
	}
	return nil
}

// ValidateRolloutOrderingEnforcedUpdate is the update-time ratchet for
// ValidateRolloutOrderingEnforced: the rules apply only when the update
// changes spec.rollout. An update that leaves the rollout untouched is
// admitted regardless of shape, so a stored object carrying an unenforced
// shape keeps reconciling (and stays mutable) until its rollout is next
// edited.
func ValidateRolloutOrderingEnforcedUpdate(oldSpec, newSpec *v1beta1.InferenceServiceSpec) error {
	var oldRollout, newRollout *v1beta1.RolloutSpec
	if oldSpec != nil {
		oldRollout = oldSpec.Rollout
	}
	if newSpec != nil {
		newRollout = newSpec.Rollout
	}
	if apiequality.Semantic.DeepEqual(oldRollout, newRollout) {
		return nil
	}
	return ValidateRolloutOrderingEnforced(newSpec)
}
