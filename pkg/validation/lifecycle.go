// Per-Component LifecycleSpec.UpdateStrategy.RollingUpdate validation.
//
// The semantic IntOrString rules (integers >= 0, percent strings
// "<n>%" with 0 <= n <= 100) live in the shared
// validateBudgetIntOrString helper in budget.go; this file composes
// the per-Component field paths and adds the zero-budget pair rule.
//
// The rollout budget reads the per-Component RollingUpdate fields
// directly (see pkg/controller/v1beta1/workload/escalation/budget.go)
// and composes them with the coordination-group budgets as independent
// layers — the effective cap is the min of both. Because the
// per-Component layer gates rollouts, the unstartable configuration
// (MaxSurge=0 AND MaxUnavailable=0) is rejected, mirroring
// appsv1.Deployment's rule. Either knob nil (operator unset) is fine —
// nil means "this layer does not cap" so the rollout falls through to
// the group layer.
//
// The webhook ALSO rejects the equivalent deadlock at the
// coordination-pacing layer (see validatePacingNotZeroBudget in
// coordination.go) so the operator-visible blast radius is consistent
// across both layers.
package validation

import (
	"fmt"

	"k8s.io/apimachinery/pkg/util/intstr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// Admission rejection reasons for per-Component lifecycle settings.
const (
	ReasonInvalidRollingUpdatePercent = "InvalidRollingUpdatePercent"
	ReasonInvalidRollingUpdateInteger = "InvalidRollingUpdateInteger"
	// ReasonRollingUpdateZeroBudget rejects MaxSurge=0 AND MaxUnavailable=0
	// set explicitly on the same Component's rollingUpdate block — the
	// rollout would be unstartable (no surge headroom, no drain headroom).
	// Mirrors the upstream appsv1.Deployment validation rule. Either
	// field nil (defaulted upstream) does NOT trip this check.
	ReasonRollingUpdateZeroBudget = "RollingUpdateZeroBudget"
	// ReasonInvalidMinReadySeconds rejects a negative lifecycle.minReadySeconds.
	// The CRD schema carries the same Minimum=0 bound; the pure validator
	// keeps the rule enforceable off-cluster.
	ReasonInvalidMinReadySeconds = "InvalidMinReadySeconds"
)

// ValidateLifecycle inspects every Component's LifecycleSpec on the
// InferenceService: UpdateStrategy.RollingUpdate.MaxUnavailable and
// .MaxSurge (both *intstr.IntOrString) and MinReadySeconds.
// Wired into the webhook from inference_service_validation.go.
func ValidateLifecycle(spec *v1beta1.InferenceServiceSpec) error {
	for _, r := range declaredRoles(spec) {
		if err := validateComponentLifecycle(string(r.kind), r.extension.Lifecycle); err != nil {
			return err
		}
	}
	return nil
}

func validateComponentLifecycle(name string, lc *v1beta1.LifecycleSpec) error {
	if lc == nil {
		return nil
	}
	if lc.MinReadySeconds != nil && *lc.MinReadySeconds < 0 {
		return fmt.Errorf("spec.%s.lifecycle.minReadySeconds must be >= 0, got %d (%s)",
			name, *lc.MinReadySeconds, ReasonInvalidMinReadySeconds)
	}
	if lc.UpdateStrategy == nil || lc.UpdateStrategy.RollingUpdate == nil {
		return nil
	}
	ru := lc.UpdateStrategy.RollingUpdate
	base := fmt.Sprintf("spec.%s.lifecycle.updateStrategy.rollingUpdate.", name)
	if err := validateBudgetIntOrString(base+"maxUnavailable", ru.MaxUnavailable); err != nil {
		return err
	}
	if err := validateBudgetIntOrString(base+"maxSurge", ru.MaxSurge); err != nil {
		return err
	}
	if err := validateRollingUpdateNotBothZero(name, ru.MaxSurge, ru.MaxUnavailable); err != nil {
		return err
	}
	return nil
}

// validateRollingUpdateNotBothZero rejects maxSurge and maxUnavailable
// both set to zero on one Component's rollingUpdate block: with no surge
// headroom and no drain headroom the rollout can never start. The rule
// holds for in-place strategies too, because the pod is marked not ready
// while its container is patched and that consumes maxUnavailable.
// Either field nil does not trip the rule: nil means "no cap at this
// layer" and the coordination-group layer's non-zero defaults apply.
// Both values have passed validateBudgetIntOrString, so a budget that
// resolves to zero is an explicit zero.
func validateRollingUpdateNotBothZero(component string, maxSurge, maxUnavailable *intstr.IntOrString) error {
	if !budgetResolvesToZero(maxSurge) || !budgetResolvesToZero(maxUnavailable) {
		return nil
	}
	return fmt.Errorf("spec.%s.lifecycle.updateStrategy.rollingUpdate: maxSurge=0 and maxUnavailable=0 together yield an unstartable rollout (%s)",
		component, ReasonRollingUpdateZeroBudget)
}
