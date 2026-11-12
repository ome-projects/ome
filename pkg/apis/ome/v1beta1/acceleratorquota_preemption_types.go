package v1beta1

// AcceleratorQuotaPreemptionPolicy selects which workloads a pending workload
// may preempt. Each field accepts only the policies meaningful for that scope.
type AcceleratorQuotaPreemptionPolicy string

const (
	// AcceleratorQuotaPreemptionNever prevents preemption in the selected scope.
	AcceleratorQuotaPreemptionNever AcceleratorQuotaPreemptionPolicy = "Never"
	// AcceleratorQuotaPreemptionLowerPriority permits preemption of lower-priority workloads.
	AcceleratorQuotaPreemptionLowerPriority AcceleratorQuotaPreemptionPolicy = "LowerPriority"
	// AcceleratorQuotaPreemptionAny permits reclamation regardless of workload priority.
	AcceleratorQuotaPreemptionAny AcceleratorQuotaPreemptionPolicy = "Any"
	// AcceleratorQuotaPreemptionLowerOrNewerEqualPriority also permits preemption of newer workloads with equal priority.
	AcceleratorQuotaPreemptionLowerOrNewerEqualPriority AcceleratorQuotaPreemptionPolicy = "LowerOrNewerEqualPriority"
)

// AcceleratorQuotaPreemption configures Kueue preemption for one ClusterQueue.
// These policies authorize evictions for incoming workloads in this queue.
// They do not protect its running workloads from another queue's reclamation.
//
// +kubebuilder:validation:XValidation:rule="!has(self.borrowWithinCohort) || !has(self.borrowWithinCohort.policy) || self.borrowWithinCohort.policy != 'LowerPriority' || (has(self.reclaimWithinCohort) && self.reclaimWithinCohort in ['Any', 'LowerPriority'])",message="borrowWithinCohort.policy LowerPriority requires reclaimWithinCohort Any or LowerPriority"
type AcceleratorQuotaPreemption struct {
	// ReclaimWithinCohort permits eviction of eligible borrowers to recover
	// this queue's entitlement. Any ignores the selected workload's priority;
	// LowerPriority requires a lower-priority workload. Unset keeps Kueue's Never default.
	// +optional
	// +kubebuilder:validation:Enum=Never;LowerPriority;Any
	ReclaimWithinCohort AcceleratorQuotaPreemptionPolicy `json:"reclaimWithinCohort,omitempty"`

	// WithinClusterQueue permits eviction inside this queue. LowerPriority
	// requires a lower-priority workload; LowerOrNewerEqualPriority also permits
	// newer equal-priority workloads. Unset keeps Kueue's Never default.
	// +optional
	// +kubebuilder:validation:Enum=Never;LowerPriority;LowerOrNewerEqualPriority
	WithinClusterQueue AcceleratorQuotaPreemptionPolicy `json:"withinClusterQueue,omitempty"`

	// BorrowWithinCohort permits eviction to obtain borrowed quota. Ordinary
	// borrowing of idle quota does not require this. Kueue does not support
	// borrowing-driven preemption with Fair Sharing enabled.
	// +optional
	BorrowWithinCohort *AcceleratorQuotaBorrowWithinCohort `json:"borrowWithinCohort,omitempty"`
}

// AcceleratorQuotaBorrowWithinCohort configures preemption when an incoming
// workload needs borrowed quota rather than its queue's own entitlement.
type AcceleratorQuotaBorrowWithinCohort struct {
	// Policy permits eviction of lower-priority borrowers when LowerPriority.
	// Unset keeps Kueue's Never default.
	// +optional
	// +kubebuilder:validation:Enum=Never;LowerPriority
	Policy AcceleratorQuotaPreemptionPolicy `json:"policy,omitempty"`

	// MaxPriorityThreshold limits borrowing-driven preemption to workloads whose
	// priority is at most this value. It has no effect when Policy is Never.
	// Unset adds no threshold beyond the policy's priority comparison.
	// +optional
	MaxPriorityThreshold *int32 `json:"maxPriorityThreshold,omitempty"`
}
