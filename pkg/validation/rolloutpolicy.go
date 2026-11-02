package validation

import (
	"fmt"

	"k8s.io/apimachinery/pkg/util/intstr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/rolloutpolicy"
)

// ReasonRolloutPolicyInvalid is the admission rejection reason for a
// RolloutPolicy whose progression body fails the plan rules an inline block
// faces, or one of the policy-only portability restrictions below.
const ReasonRolloutPolicyInvalid = "RolloutPolicyInvalid"

// ValidateRolloutPolicySpec validates a RolloutPolicy body standalone. Two
// rule families:
//
//  1. The plan-body rules an inline progression faces (shared functions, so
//     policy admission and ISVC admission can never disagree about what is
//     admissible).
//  2. Policy-only portability restrictions — a policy body is fleet data that
//     rides to any consumer on any cluster, so it must not carry
//     service-specific or cluster-local values:
//     - capacities must be percentages (absolute counts are service-specific,
//     and the final-step-completeness rule is only checkable for percent);
//     - the metrics source must be a logical providerRef (a raw serverAddress
//     or per-service authRef is cluster-local and an SSRF surface);
//     - providerRef is required when any step declares analysis, so a policy
//     can never ship a gate with no resolvable source.
//
// Shape-dependent rules (entrypoint, orphan, collapse, soak arity) do NOT run
// here — they evaluate on the consumer at ISVC admission, over the ref's
// declared progression kind.
func ValidateRolloutPolicySpec(spec *v1beta1.RolloutPolicySpec) error {
	if spec == nil {
		return fmt.Errorf("spec must not be empty (%s)", ReasonRolloutPolicyInvalid)
	}
	// Value-level mirror of the CRD's exactly-one CEL rule, for callers that
	// bypass schema validation.
	bodies := rolloutpolicy.ProgressionBodies(spec.Canary, spec.BlueGreen, spec.RollingUpdate)
	if len(bodies) != 1 {
		return fmt.Errorf("spec must set exactly one of canary, blueGreen, or rollingUpdate; got %d (%s)", len(bodies), ReasonRolloutPolicyInvalid)
	}
	switch body := bodies[0].Body.(type) {
	case *v1beta1.GroupCanary:
		return validateRolloutPolicyCanary(body)
	case *v1beta1.GroupRollingUpdate:
		return validateRolloutPolicyRollingUpdate(body)
	}
	// blueGreen carries no fields today.
	return nil
}

func validateRolloutPolicyCanary(c *v1beta1.GroupCanary) error {
	if err := ValidateCanaryPlan("spec.canary", c); err != nil {
		return err
	}
	hasAnalysis := false
	for i, s := range c.Steps {
		if s.Analysis != nil {
			hasAnalysis = true
		}
		if s.Capacity.Type != intstr.String {
			return fmt.Errorf("spec.canary.steps[%d].capacity: a policy capacity must be a percentage (%q is an absolute count, which is service-specific — services needing absolute counts keep their canary inline) (%s)",
				i, s.Capacity.String(), ReasonRolloutPolicyInvalid)
		}
	}
	if p := c.Prometheus; p != nil {
		if p.ServerAddress != "" {
			return fmt.Errorf("spec.canary.prometheus.serverAddress is not allowed in a policy body — a cluster-local URL defeats portability; name a providerRef bound in the operator's metricProviders configuration instead (%s)",
				ReasonRolloutPolicyInvalid)
		}
		if p.AuthRef != nil {
			return fmt.Errorf("spec.canary.prometheus.authRef is not allowed in a policy body — auth comes from the providerRef's cluster binding (%s)",
				ReasonRolloutPolicyInvalid)
		}
	}
	if hasAnalysis && (c.Prometheus == nil || c.Prometheus.ProviderRef == nil || c.Prometheus.ProviderRef.Name == "") {
		return fmt.Errorf("spec.canary.prometheus.providerRef is required when any step declares analysis — a policy must never ship a gate with no resolvable metrics source (%s)",
			ReasonRolloutPolicyInvalid)
	}
	return nil
}

func validateRolloutPolicyRollingUpdate(ru *v1beta1.GroupRollingUpdate) error {
	if err := validateBudgetIntOrString("spec.rollingUpdate.maxSurge", ru.MaxSurge); err != nil {
		return err
	}
	if err := validateBudgetIntOrString("spec.rollingUpdate.maxUnavailable", ru.MaxUnavailable); err != nil {
		return err
	}
	if budgetResolvesToZero(ru.MaxSurge) && budgetResolvesToZero(ru.MaxUnavailable) {
		return fmt.Errorf("spec.rollingUpdate: maxSurge and maxUnavailable both resolve to zero, which deadlocks the rollout — set at least one non-zero (%s)",
			ReasonZeroBudgetPacingUnstartable)
	}
	return nil
}
