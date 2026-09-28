package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// PlacementPlanStatus identifies the allocation persisted before member writes.
// Original assignments remain fixed throughout a transition, including retargets.
type PlacementPlanStatus struct {
	ID string `json:"id"`
	// Revision increases for every accepted allocation or drain instruction.
	// +kubebuilder:validation:Minimum=1
	Revision  int64     `json:"revision"`
	SourceUID types.UID `json:"sourceUID"`
	// +kubebuilder:validation:Minimum=1
	ObservedGeneration int64 `json:"observedGeneration"`
	// InputDigest identifies resolved source intent and accepted allocation inputs.
	// Heartbeat timestamps and report resource versions do not change this digest.
	InputDigest string `json:"inputDigest"`
	// PauseSurge keeps new member rollout reservations stopped while placement
	// owns the shared transition allowance. Releasing it creates a new revision.
	PauseSurge bool `json:"pauseSurge"`
	// RequestedReplicas includes the assigned and unassigned desired floor.
	// +kubebuilder:validation:Minimum=0
	RequestedReplicas int64 `json:"requestedReplicas"`
	// +kubebuilder:validation:Minimum=0
	AssignedReplicas int64 `json:"assignedReplicas"`
	// +kubebuilder:validation:Minimum=0
	UnassignedReplicas int32 `json:"unassignedReplicas"`
	// OriginalUnassignedReplicas retains the unassigned part of the original floor.
	// +kubebuilder:validation:Minimum=0
	OriginalUnassignedReplicas int32 `json:"originalUnassignedReplicas"`
}

// CandidateAllocationStatus separates original, currently authorized, and desired
// floors. A zero current floor does not authorize deletion before traffic drains.
type CandidateAllocationStatus struct {
	ClusterUID types.UID `json:"clusterUID"`
	// MatchingTerms contains zero-based indexes into the source's affinity terms.
	// +optional
	// +listType=set
	MatchingTerms []int32 `json:"matchingTerms,omitempty"`
	// Weight is the effective static allocation weight, when applicable.
	// +optional
	// +kubebuilder:validation:Minimum=0
	Weight *int64 `json:"weight,omitempty"`
	// Capacity is the accepted hardware sample, when capacity determines weight.
	// +optional
	Capacity *PlacementCapacitySample `json:"capacity,omitempty"`
	// +kubebuilder:validation:Minimum=0
	OriginalReplicas int32 `json:"originalReplicas"`
	// +kubebuilder:validation:Minimum=0
	CurrentReplicas int32 `json:"currentReplicas"`
	// +kubebuilder:validation:Minimum=0
	DesiredReplicas int32 `json:"desiredReplicas"`
	// DrainRequested withdraws this home from routing before physical removal.
	// +optional
	DrainRequested bool `json:"drainRequested,omitempty"`
}

// PlacementCapacitySample records the identified hardware and resolved demand
// used to normalize one member into nominal whole-replica capacity.
type PlacementCapacitySample struct {
	DemandFingerprint string `json:"demandFingerprint"`
	// +kubebuilder:validation:Minimum=0
	Replicas int64 `json:"replicas"`
	// +listType=map
	// +listMapKey=resourceName
	// +listMapKey=resourceFlavor
	Pools []PlacementCapacityPool `json:"pools"`
}

// PlacementCapacityPool is the evidence for one resource/flavor ratio.
type PlacementCapacityPool struct {
	ResourceName   string `json:"resourceName"`
	ResourceFlavor string `json:"resourceFlavor"`
	// +kubebuilder:validation:Minimum=1
	Demand int64 `json:"demand"`
	// +kubebuilder:validation:Minimum=0
	Allocatable           int64                          `json:"allocatable"`
	ObservedAt            metav1.Time                    `json:"observedAt"`
	ReportUID             types.UID                      `json:"reportUID"`
	ReportResourceVersion string                         `json:"reportResourceVersion"`
	Attribution           AcceleratorCapacityAttribution `json:"attribution"`
}

// PlacementExecutionPolicy carries source allocation authority to a member's
// component reconciler. It is absent for locally managed services.
type PlacementExecutionPolicy struct {
	PlanID string `json:"planID"`
	// +kubebuilder:validation:Minimum=1
	Revision   int64     `json:"revision"`
	SourceUID  types.UID `json:"sourceUID"`
	ClusterUID types.UID `json:"clusterUID"`
	// PauseSurge prevents new surplus-producing operations while existing
	// operations retain their reservations and may finish cleanup.
	PauseSurge bool `json:"pauseSurge"`
}
