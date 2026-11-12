package tree

import (
	"slices"
	"strings"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// checkPreemption also runs when the installed CRD omits structural validation.
// Invalid policies must be rejected before they reach Kueue.
func checkPreemption(t *Tree) Violations {
	var vs Violations
	for _, n := range t.Nodes() {
		policy := n.Quota.Spec.Preemption
		if policy == nil {
			continue
		}
		if !n.IsLeaf() || n == t.Root {
			vs = append(vs, violation(n.Name(), v1beta1.AcceleratorQuotaReasonNodeKindInvalid, "preemption",
				"only a non-root ClusterQueue node may set preemption"))
			continue
		}
		vs = append(vs, checkQueuePreemption(n.Name(), policy)...)
	}
	return vs
}

func checkQueuePreemption(name string, policy *v1beta1.AcceleratorQuotaPreemption) Violations {
	vs := checkPreemptionPolicy(name, "reclaimWithinCohort", policy.ReclaimWithinCohort,
		v1beta1.AcceleratorQuotaPreemptionNever, v1beta1.AcceleratorQuotaPreemptionLowerPriority,
		v1beta1.AcceleratorQuotaPreemptionAny)
	vs = append(vs, checkPreemptionPolicy(name, "withinClusterQueue", policy.WithinClusterQueue,
		v1beta1.AcceleratorQuotaPreemptionNever, v1beta1.AcceleratorQuotaPreemptionLowerPriority,
		v1beta1.AcceleratorQuotaPreemptionLowerOrNewerEqualPriority)...)
	borrow := policy.BorrowWithinCohort
	if borrow == nil {
		return vs
	}
	vs = append(vs, checkPreemptionPolicy(name, "borrowWithinCohort.policy", borrow.Policy,
		v1beta1.AcceleratorQuotaPreemptionNever, v1beta1.AcceleratorQuotaPreemptionLowerPriority)...)
	if borrow.Policy == v1beta1.AcceleratorQuotaPreemptionLowerPriority &&
		policy.ReclaimWithinCohort != v1beta1.AcceleratorQuotaPreemptionAny &&
		policy.ReclaimWithinCohort != v1beta1.AcceleratorQuotaPreemptionLowerPriority {
		vs = append(vs, violation(name, v1beta1.AcceleratorQuotaReasonNodeKindInvalid,
			"preemption.borrowWithinCohort",
			"preemption.borrowWithinCohort.policy LowerPriority requires reclaimWithinCohort Any or LowerPriority"))
	}
	return vs
}

func checkPreemptionPolicy(name, field string, value v1beta1.AcceleratorQuotaPreemptionPolicy,
	allowed ...v1beta1.AcceleratorQuotaPreemptionPolicy) Violations {
	if value == "" || slices.Contains(allowed, value) {
		return nil
	}
	choices := make([]string, len(allowed))
	for i, policy := range allowed {
		choices[i] = string(policy)
	}
	return Violations{violation(name, v1beta1.AcceleratorQuotaReasonNodeKindInvalid, "preemption."+field,
		"preemption.%s is %q; use %s or leave it unset", field, value, strings.Join(choices, ", "))}
}
