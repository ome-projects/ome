// spec.rollout.groups[].policyRef validation (consumer side).
//
// A policyRef supplies a group's progression by reference; its declared
// progression kind is what every shape-dependent rollout rule evaluates, so
// admission stays complete without dereferencing the policy. The referenced
// body itself is validated at policy admission and again at run open.
package validation

import (
	"encoding/json"
	"fmt"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// Admission rejection reasons for spec.rollout.groups[].policyRef. Operators
// and tests reference these by name, so they're exported constants.
const (
	// ReasonRolloutPolicyRefInvalid rejects a malformed ref: an empty name,
	// a progression outside the enum, or a reserved kind.
	ReasonRolloutPolicyRefInvalid = "RolloutPolicyRefInvalid"
	// ReasonRolloutPolicyRefUnsupported rejects any ref on a cluster without
	// the rollout policy feature. An accepted-but-ignored ref would silently
	// no-op the declared gate — the failure class rollout admission exists
	// to prevent — so the ref is refused outright instead.
	ReasonRolloutPolicyRefUnsupported = "RolloutPolicyRefUnsupported"
	// ReasonRolloutPlanTooLarge rejects a progression body whose JSON
	// rendering exceeds the operator-configured cap. The effective plan is
	// pinned into consumer status at run open, so an oversized body would
	// bloat every status write for the life of the run.
	ReasonRolloutPlanTooLarge = "PlanTooLarge"
)

// RolloutPolicyKind is the only policyRef.kind admitted (and its defaulted
// value); "ClusterRolloutPolicy" is a reserved shape.
const RolloutPolicyKind = "RolloutPolicy"

// rolloutGroupKind is the progression kind a rollout group validates as, so
// every shape rule holds without dereferencing the policy.
func rolloutGroupKind(g *v1beta1.RolloutGroup) v1beta1.RolloutProgressionKind {
	return g.DeclaredProgression()
}

// ValidateRolloutPolicyRefs checks the shape of every
// spec.rollout.groups[].policyRef: the kind must be the namespaced
// RolloutPolicy (or its empty default), the name non-empty, and the declared
// progression one of the three kinds (the value-level mirror of the CRD
// enum, for callers that bypass schema validation). featureEnabled reflects
// whether this cluster runs the rollout policy surface; with it disabled ANY
// ref is rejected. Nil-safe; returns nil when no group carries a ref.
func ValidateRolloutPolicyRefs(spec *v1beta1.InferenceServiceSpec, featureEnabled bool) error {
	groups := spec.GetRolloutGroups()
	for i := range groups {
		ref := groups[i].PolicyRef
		if ref == nil {
			continue
		}
		if !featureEnabled {
			return fmt.Errorf("spec.rollout.groups[%d].policyRef: this cluster does not have the rollout policy feature enabled, so the ref would silently no-op — inline the progression instead (%s)",
				i, ReasonRolloutPolicyRefUnsupported)
		}
		if ref.Kind != "" && ref.Kind != RolloutPolicyKind {
			return fmt.Errorf("spec.rollout.groups[%d].policyRef.kind: %q is a reserved shape; only %q is supported (%s)",
				i, ref.Kind, RolloutPolicyKind, ReasonRolloutPolicyRefInvalid)
		}
		if ref.Name == "" {
			return fmt.Errorf("spec.rollout.groups[%d].policyRef.name must not be empty (%s)",
				i, ReasonRolloutPolicyRefInvalid)
		}
		switch ref.Progression {
		case v1beta1.RolloutProgressionCanary, v1beta1.RolloutProgressionBlueGreen, v1beta1.RolloutProgressionRollingUpdate:
		default:
			return fmt.Errorf("spec.rollout.groups[%d].policyRef.progression: %q must be one of canary, blueGreen, or rollingUpdate (%s)",
				i, ref.Progression, ReasonRolloutPolicyRefInvalid)
		}
	}
	return nil
}

// ValidateInlineRolloutPlanSize rejects any group whose inline progression
// body renders to more than maxBytes of JSON; maxBytes <= 0 means uncapped
// (the cap is operator configuration, never an in-code default). Inline
// plans are pinned into status at run open exactly like policy-sourced
// ones, so the same cap applies to both; a referenced policy's body is
// capped at policy admission instead.
func ValidateInlineRolloutPlanSize(spec *v1beta1.InferenceServiceSpec, maxBytes int) error {
	if maxBytes <= 0 {
		return nil
	}
	groups := spec.GetRolloutGroups()
	for i := range groups {
		g := &groups[i]
		var arm string
		var body any
		switch {
		case g.Canary != nil:
			arm, body = "canary", g.Canary
		case g.BlueGreen != nil:
			arm, body = "blueGreen", g.BlueGreen
		case g.RollingUpdate != nil:
			arm, body = "rollingUpdate", g.RollingUpdate
		default:
			continue
		}
		raw, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("spec.rollout.groups[%d].%s: cannot render the progression body: %v (%s)",
				i, arm, err, ReasonRolloutPlanTooLarge)
		}
		if len(raw) > maxBytes {
			return fmt.Errorf("spec.rollout.groups[%d].%s: progression body renders to %d bytes, exceeding the configured cap of %d — the plan is pinned into status at run open, so an oversized body would bloat every status write (%s)",
				i, arm, len(raw), maxBytes, ReasonRolloutPlanTooLarge)
		}
	}
	return nil
}
