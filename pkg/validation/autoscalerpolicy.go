package validation

import (
	"fmt"
	"strings"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/autoscalerpolicy/render"
)

// ValidateAutoscalerPolicySpec runs every cluster-state-free policy check
// (structural coherence, reserved-shape rejection, template parse +
// allowlist, sample render, PromQL parse) and joins all findings into a
// single admission error, so one rejected write surfaces the full list
// instead of forcing an issue-at-a-time fix loop. Nil when the spec is
// valid.
func ValidateAutoscalerPolicySpec(spec *v1beta1.AutoscalerPolicySpec) error {
	if spec == nil {
		return nil
	}
	issues := render.ValidateSpec(spec)
	details := make([]string, 0, len(issues)+1)
	for _, issue := range issues {
		details = append(details, issue.String())
	}
	if spec.Keda != nil {
		if err := ValidateKedaHPAName(spec.Keda.Advanced); err != nil {
			details = append(details, err.Error())
		}
	}
	if len(details) == 0 {
		return nil
	}
	return fmt.Errorf("%s", strings.Join(details, "; "))
}
