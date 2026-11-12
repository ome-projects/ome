package validation

import (
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/util/intstr"
)

// ValidatePodDisruptionBudget validates the mutually exclusive availability
// budgets accepted by a Kubernetes PodDisruptionBudget.
func ValidatePodDisruptionBudget(path string, minAvailable, maxUnavailable *intstr.IntOrString) error {
	if minAvailable != nil && maxUnavailable != nil {
		return fmt.Errorf("%s.minAvailable and %s.maxUnavailable cannot both be set", path, path)
	}
	if err := validatePodDisruptionBudgetValue(path+".minAvailable", minAvailable); err != nil {
		return err
	}
	return validatePodDisruptionBudgetValue(path+".maxUnavailable", maxUnavailable)
}

// validatePodDisruptionBudgetValue admits an integer at or above zero or a
// percent from "0%" to "100%" written in digits only, so a signed percent
// is refused. One text names every refusal.
func validatePodDisruptionBudgetValue(path string, value *intstr.IntOrString) error {
	if value == nil {
		return nil
	}
	n, percent, err := intOrPercent(value)
	if err != nil || n < 0 || (percent && (n > 100 || strings.ContainsAny(value.StrVal, "+-"))) {
		return fmt.Errorf("%s must be a non-negative integer or a percentage from 0%% to 100%%", path)
	}
	return nil
}
