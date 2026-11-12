package kueue

import (
	kueuev1beta2 "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	kueueac "sigs.k8s.io/kueue/client-go/applyconfiguration/kueue/v1beta2"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// renderPreemption owns only the fields the quota author set. Kueue supplies
// defaults for absent policies, and another field manager may own them.
func renderPreemption(policy *v1beta1.AcceleratorQuotaPreemption) *kueueac.ClusterQueuePreemptionApplyConfiguration {
	if policy == nil {
		return nil
	}
	out := kueueac.ClusterQueuePreemption()
	if policy.ReclaimWithinCohort != "" {
		out = out.WithReclaimWithinCohort(kueuev1beta2.PreemptionPolicy(policy.ReclaimWithinCohort))
	}
	if policy.WithinClusterQueue != "" {
		out = out.WithWithinClusterQueue(kueuev1beta2.PreemptionPolicy(policy.WithinClusterQueue))
	}
	if borrow := renderBorrowPreemption(policy.BorrowWithinCohort); borrow != nil {
		out = out.WithBorrowWithinCohort(borrow)
	}
	if out.ReclaimWithinCohort == nil && out.WithinClusterQueue == nil && out.BorrowWithinCohort == nil {
		return nil
	}
	return out
}

func renderBorrowPreemption(policy *v1beta1.AcceleratorQuotaBorrowWithinCohort) *kueueac.BorrowWithinCohortApplyConfiguration {
	if policy == nil || (policy.Policy == "" && policy.MaxPriorityThreshold == nil) {
		return nil
	}
	out := kueueac.BorrowWithinCohort()
	if policy.Policy != "" {
		out = out.WithPolicy(kueuev1beta2.BorrowWithinCohortPolicy(policy.Policy))
	}
	if policy.MaxPriorityThreshold != nil {
		out = out.WithMaxPriorityThreshold(*policy.MaxPriorityThreshold)
	}
	return out
}
