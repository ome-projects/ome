// Route-provider capability validation.
//
// Maps the per-revision traffic intent an InferenceService declares (a
// canary's step traffic percentages) onto the capability tokens the
// configured route provider declares (pkg/constants RouteCapability*),
// and enforces the admission-time half of the contract. The rollout run
// layer reuses the same requirement and comparison helpers to hold a
// run the provider cannot carry and to report a provider whose
// capabilities are not declared, so the two views cannot drift.
package validation

import (
	"fmt"
	"reflect"

	apiequality "k8s.io/apimachinery/pkg/api/equality"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/rollout"
)

// ReasonRouteProviderUnsupported is the admission rejection reason for
// traffic intent the configured route provider declares it cannot express.
// The rollout run layer holds a stored InferenceService carrying such
// intent under the same reason.
const ReasonRouteProviderUnsupported = "RouteProviderUnsupported"

// RouteProviderCapabilities is the declared capability set of the route
// provider an InferenceService's external route resolves to. Provider
// names the provider in messages. Known is false when the provider
// declares no capability set at all: nothing is rejected then, and the
// controller reports the intent as unverified instead.
type RouteProviderCapabilities struct {
	Provider     string
	Capabilities []string
	Known        bool
}

// Has reports whether the provider declares capability.
func (c RouteProviderCapabilities) Has(capability string) bool {
	for _, have := range c.Capabilities {
		if have == capability {
			return true
		}
	}
	return false
}

// RouteCapabilityRequirement is one piece of spec intent and the
// route-provider capability it needs.
type RouteCapabilityRequirement struct {
	// Field is the spec path that carries the intent, for messages.
	Field string
	// Capability is the pkg/constants RouteCapability* token the field needs.
	Capability string
}

// RouteEntrypoint is the Component an InferenceService's external route
// targets: the router when the spec declares one, otherwise the engine.
// Nil-safe.
func RouteEntrypoint(spec *v1beta1.InferenceServiceSpec) v1beta1.ComponentType {
	if spec != nil && spec.Router != nil {
		return v1beta1.RouterComponent
	}
	return v1beta1.EngineComponent
}

// RequiredRouteCapabilities lists what the spec's traffic intent asks of
// the route provider, in group order. A canary-kind group covering the
// entrypoint's unit programs a per-revision split on the external route,
// so its steps' traffic percentages need weighted revision routing. A
// canary on a unit behind the router shifts traffic by capacity through
// the router's own balancing and asks nothing of the provider. Nil-safe;
// nil when the spec carries no such intent.
func RequiredRouteCapabilities(spec *v1beta1.InferenceServiceSpec) []RouteCapabilityRequirement {
	var out []RouteCapabilityRequirement
	groups := spec.GetRolloutGroups()
	entrypoint := RouteEntrypoint(spec)
	for gi := range groups {
		out = append(out, RouteRequirementsOfGroup(fmt.Sprintf("spec.rollout.groups[%d]", gi), &groups[gi], entrypoint)...)
	}
	return out
}

// RouteRequirementsOfGroup returns the route-provider capabilities one
// rollout group asks for. where names the group in messages; entrypoint is
// the Component the external route targets (RouteEntrypoint). A canary
// group whose units include the entrypoint's requires weighted revision
// routing, whether its plan is inline or referenced.
func RouteRequirementsOfGroup(where string, g *v1beta1.RolloutGroup, entrypoint v1beta1.ComponentType) []RouteCapabilityRequirement {
	if g == nil || g.DeclaredProgression() != v1beta1.RolloutProgressionCanary {
		return nil
	}
	unit := rollout.CanaryUnit(entrypoint)
	for _, u := range rollout.CanaryUnitsOf(g) {
		if u != unit {
			continue
		}
		field := where + ".canary.steps[].traffic"
		if g.Canary == nil {
			field = where + ".policyRef (progression canary)"
		}
		return []RouteCapabilityRequirement{{Field: field, Capability: constants.RouteCapabilityWeightedRevisionRouting}}
	}
	return nil
}

// UnsupportedRouteCapabilities returns the requirements a provider with a
// declared capability set does not cover. Nil when the set is unknown:
// an undeclared provider gates nothing and is reported instead.
func UnsupportedRouteCapabilities(required []RouteCapabilityRequirement, caps RouteProviderCapabilities) []RouteCapabilityRequirement {
	if !caps.Known {
		return nil
	}
	var out []RouteCapabilityRequirement
	for _, r := range required {
		if !caps.Has(r.Capability) {
			out = append(out, r)
		}
	}
	return out
}

// RouteCapabilityUnsupportedError is the operator-facing error for one
// requirement the provider does not declare. Admission returns it and the
// rollout run layer holds with its text.
func RouteCapabilityUnsupportedError(req RouteCapabilityRequirement, caps RouteProviderCapabilities) error {
	return fmt.Errorf("%s splits traffic between revisions by percentage, but the configured route provider %q does not declare the %s capability (ingress.routeProviderCapabilities); declare it for that provider, route through one that has it, or use a blueGreen/rollingUpdate progression (%s)",
		req.Field, caps.Provider, req.Capability, ReasonRouteProviderUnsupported)
}

// ValidateRouteCapabilities is the admission-time gate: it rejects the
// first requirement of the spec's traffic intent that a provider with a
// declared capability set does not cover. A provider with no declared set
// admits everything (the controller reports the intent as unverified).
func ValidateRouteCapabilities(spec *v1beta1.InferenceServiceSpec, caps RouteProviderCapabilities) error {
	unsupported := UnsupportedRouteCapabilities(RequiredRouteCapabilities(spec), caps)
	if len(unsupported) == 0 {
		return nil
	}
	return RouteCapabilityUnsupportedError(unsupported[0], caps)
}

// ValidateRouteCapabilitiesUpdate is the update-time ratchet for
// ValidateRouteCapabilities: the gate applies only when the update touches
// spec.rollout, changes what the intent asks for (a router added in front
// of the engine moves the external route), or changes the provider it asks.
// An update that leaves all three alone is admitted, so a stored
// InferenceService whose provider was declared incapable after it was
// written keeps accepting unrelated edits while the controller holds and
// reports its rollout.
func ValidateRouteCapabilitiesUpdate(oldSpec, newSpec *v1beta1.InferenceServiceSpec, oldCaps, newCaps RouteProviderCapabilities) error {
	var oldRollout, newRollout *v1beta1.RolloutSpec
	if oldSpec != nil {
		oldRollout = oldSpec.Rollout
	}
	if newSpec != nil {
		newRollout = newSpec.Rollout
	}
	if oldCaps.Provider == newCaps.Provider &&
		apiequality.Semantic.DeepEqual(oldRollout, newRollout) &&
		reflect.DeepEqual(RequiredRouteCapabilities(oldSpec), RequiredRouteCapabilities(newSpec)) {
		return nil
	}
	return ValidateRouteCapabilities(newSpec, newCaps)
}
