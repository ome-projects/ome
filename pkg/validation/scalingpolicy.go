// ScalingPolicy admission validation shared by InferenceServices and runtimes.
package validation

import (
	"fmt"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// ReasonScalingModeNotImplemented is the admission rejection reason for a
// ScalingPolicy mode the API accepts but no controller applies. Operators and
// tests reference it by name, so it's an exported constant.
const ReasonScalingModeNotImplemented = "ScalingModeNotImplemented"

// ValidateScalingPolicy rejects modes that are not ready for admission.
// fieldPath names the field in error messages (e.g. "spec.scalingPolicy").
func ValidateScalingPolicy(policy *v1beta1.ScalingPolicy, fieldPath string) error {
	if policy == nil {
		return nil
	}
	switch policy.Mode {
	case "", v1beta1.ScalingIndependent:
		if policy.Proportional != nil {
			return fmt.Errorf("%s.proportional requires mode %q", fieldPath, v1beta1.ScalingProportional)
		}
		return nil
	case v1beta1.ScalingProportional:
		return ValidateProportionalShape(policy, fieldPath)
	case v1beta1.ScalingPinned:
		return fmt.Errorf(
			"%s.mode %q is accepted by the API but not implemented; use %q or omit the policy (%s)",
			fieldPath, policy.Mode, v1beta1.ScalingIndependent,
			ReasonScalingModeNotImplemented)
	default:
		// The CRD enum already rejects unknown modes; this covers callers
		// that bypass the schema (e.g. direct Go API use).
		return fmt.Errorf("%s.mode %q is not one of %s|%s|%s",
			fieldPath, policy.Mode,
			v1beta1.ScalingIndependent, v1beta1.ScalingProportional, v1beta1.ScalingPinned)
	}
}

// ValidateProportionalShape enforces the exact integer ratio contract.
func ValidateProportionalShape(policy *v1beta1.ScalingPolicy, fieldPath string) error {
	if policy == nil || policy.Proportional == nil || len(policy.Proportional.Ratios) == 0 {
		return fmt.Errorf("%s.proportional.ratios must contain a follower", fieldPath)
	}
	anchor := policy.Proportional.Anchor
	if !validScalingComponent(anchor) {
		return fmt.Errorf("%s.proportional.anchor %q is not a component", fieldPath, anchor)
	}
	for component, ratio := range policy.Proportional.Ratios {
		if !validScalingComponent(component) || component == anchor {
			return fmt.Errorf("%s.proportional.ratios[%q] must name a follower distinct from the anchor", fieldPath, component)
		}
		milli := ratio.MilliValue()
		if ratio.Sign() <= 0 || milli <= 0 || milli > 1_000_000 || ratio.Cmp(*resource.NewMilliQuantity(milli, resource.DecimalSI)) != 0 {
			return fmt.Errorf("%s.proportional.ratios[%q] must be in (0, 1000] with at most three decimal places", fieldPath, component)
		}
	}
	return nil
}

func validScalingComponent(component v1beta1.ComponentType) bool {
	return isRole(component)
}

// validateScalingPolicyComponents checks conflicting explicitly declared
// follower autoscalers. Missing component blocks may come from the runtime;
// the reconciler validates their resolved presence before changing a count.
func validateScalingPolicyComponents(policy *v1beta1.ScalingPolicy, components map[v1beta1.ComponentType]*v1beta1.ComponentExtensionSpec) error {
	if policy == nil || policy.Mode != v1beta1.ScalingProportional || policy.Proportional == nil {
		return nil
	}
	for component := range policy.Proportional.Ratios {
		block := components[component]
		if block == nil {
			continue
		}
		if block.AutoscalerPolicyRef != nil {
			return fmt.Errorf("%s follower cannot reference AutoscalerPolicy %q when scaling is Proportional: remove the reference or remove %s from ratios",
				component, block.AutoscalerPolicyRef.Name, component)
		}
		if block.Autoscaler == nil {
			continue
		}
		if block.Autoscaler.Class != v1beta1.AutoscalerNone {
			return fmt.Errorf("%s follower autoscaler must have class None when scaling is Proportional", component)
		}
	}
	return nil
}

// ValidateScalingPolicyService rejects explicit modes that cannot own a
// follower's InferenceReplica and checks visible component autoscalers.
func ValidateScalingPolicyService(isvc *v1beta1.InferenceService) error {
	if isvc == nil || isvc.Spec.ScalingPolicy == nil || isvc.Spec.ScalingPolicy.Mode != v1beta1.ScalingProportional {
		return nil
	}
	if err := ValidateScalingPolicy(isvc.Spec.ScalingPolicy, "spec.scalingPolicy"); err != nil {
		return err
	}
	if isvc.Spec.ReplicaRefs != nil {
		return fmt.Errorf("Proportional scaling requires InferenceService-owned replicas")
	}
	return validateScalingComponents(isvc.Spec.ScalingPolicy, declaredRoles(&isvc.Spec), isvc.Spec.DeploymentMode)
}

// ValidateScalingPolicyRuntime checks the declared component defaults.
func ValidateScalingPolicyRuntime(runtime *v1beta1.ServingRuntimeSpec) error {
	if runtime == nil || runtime.ScalingPolicy == nil || runtime.ScalingPolicy.Mode != v1beta1.ScalingProportional {
		return nil
	}
	if err := ValidateScalingPolicy(runtime.ScalingPolicy, "spec.scalingPolicy"); err != nil {
		return err
	}
	return validateScalingComponents(runtime.ScalingPolicy, runtimeRoles(runtime), nil)
}

func validateScalingComponents(policy *v1beta1.ScalingPolicy, roles roleList, defaultMode *constants.DeploymentModeType) error {
	components := map[v1beta1.ComponentType]*v1beta1.ComponentExtensionSpec{}
	modes := map[v1beta1.ComponentType]string{}
	for _, r := range roles {
		components[r.kind] = r.extension
		modes[r.kind] = r.deploymentModeAnnotation()
	}
	if components[policy.Proportional.Anchor] == nil {
		return fmt.Errorf("spec.scalingPolicy.proportional.anchor %q requires a declared component", policy.Proportional.Anchor)
	}
	for follower := range policy.Proportional.Ratios {
		if components[follower] == nil {
			return fmt.Errorf("spec.scalingPolicy.proportional.ratios[%q] requires a declared component", follower)
		}
	}
	for component := range policy.Proportional.Ratios {
		if err := validateScalingMode(component, modes[component], defaultMode); err != nil {
			return err
		}
	}
	if err := validateScalingMode(policy.Proportional.Anchor, modes[policy.Proportional.Anchor], defaultMode); err != nil {
		return err
	}
	return validateScalingPolicyComponents(policy, components)
}

func validateScalingMode(component v1beta1.ComponentType, explicit string, defaultMode *constants.DeploymentModeType) error {
	if explicit == "" && defaultMode != nil {
		explicit = string(*defaultMode)
	}
	if explicit != "" && explicit != string(constants.OMENative) {
		return fmt.Errorf("Proportional scaling requires OMENative %s, not deployment mode %q", component, explicit)
	}
	return nil
}

// Update validation revisits ownership when the policy or one of its writers
// changes. Unrelated updates to stored policies do not revalidate old state.
func ValidateScalingPolicyServiceUpdate(old, current *v1beta1.InferenceService) error {
	if old != nil && apiequality.Semantic.DeepEqual(old.Spec.ScalingPolicy, current.Spec.ScalingPolicy) &&
		apiequality.Semantic.DeepEqual(old.Spec.ReplicaRefs, current.Spec.ReplicaRefs) &&
		apiequality.Semantic.DeepEqual(old.Spec.DeploymentMode, current.Spec.DeploymentMode) &&
		!scalingOwnersChanged(declaredRoles(&old.Spec), declaredRoles(&current.Spec)) {
		return nil
	}
	return ValidateScalingPolicyService(current)
}

func ValidateScalingPolicyRuntimeUpdate(old, current *v1beta1.ServingRuntimeSpec) error {
	if old != nil && apiequality.Semantic.DeepEqual(old.ScalingPolicy, current.ScalingPolicy) &&
		!scalingOwnersChanged(runtimeRoles(old), runtimeRoles(current)) {
		return nil
	}
	return ValidateScalingPolicyRuntime(current)
}

// scalingOwnersChanged reports whether the two lists declare different roles,
// or a role's deploymentMode annotation, autoscaler or autoscalerPolicyRef
// differs between them.
func scalingOwnersChanged(old, current roleList) bool {
	if len(old) != len(current) {
		return true
	}
	for i := range old {
		if old[i].kind != current[i].kind || scalingOwnerChanged(&old[i], &current[i]) {
			return true
		}
	}
	return false
}

func scalingOwnerChanged(old, current *declaredRole) bool {
	return old.deploymentModeAnnotation() != current.deploymentModeAnnotation() ||
		!apiequality.Semantic.DeepEqual(old.extension.Autoscaler, current.extension.Autoscaler) ||
		!apiequality.Semantic.DeepEqual(old.extension.AutoscalerPolicyRef, current.extension.AutoscalerPolicyRef)
}

// ValidateScalingPolicyUpdate runs ValidateScalingPolicy only when this write
// sets or changes the policy. An unchanged policy — however it was admitted —
// never blocks the update, so objects stored with a now-rejected mode keep
// reconciling and accepting unrelated spec changes. Equality is semantic, so
// rewriting a ratio quantity in an equivalent form (e.g. "1" vs "1000m") does
// not count as a change.
func ValidateScalingPolicyUpdate(oldPolicy, newPolicy *v1beta1.ScalingPolicy, fieldPath string) error {
	if apiequality.Semantic.DeepEqual(oldPolicy, newPolicy) {
		return nil
	}
	return ValidateScalingPolicy(newPolicy, fieldPath)
}
