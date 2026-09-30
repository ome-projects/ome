package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// PlacementInputValid reports whether source placement intent can be evaluated.
// Serving readiness is independent of this condition.
const PlacementInputValid = "PlacementInputValid"

// PlacementSatisfied reports whether every assigned floor is admitted and no
// requested floor remains unassigned. Serving readiness is independent.
const PlacementSatisfied = "PlacementSatisfied"

// PlacementConverged reports whether member execution has reached the accepted
// allocation. It does not imply admission or serving readiness.
const PlacementConverged = "PlacementConverged"

// PlacementCapacityFresh reports whether this reconcile verified usable hardware
// and resolved demand for every matched capacity-placement member.
const PlacementCapacityFresh = "PlacementCapacityFresh"

// PlacementBackendReady reports whether all participating components resolve to
// OMENative. It is advisory and does not change observed serving readiness.
const PlacementBackendReady = "PlacementBackendReady"

// PlacementReplicaFloorsReady reports whether resolved member floors match
// placement authority. It does not change observed serving readiness.
const PlacementReplicaFloorsReady = "PlacementReplicaFloorsReady"

// PlacementPlanStatus identifies the allocation persisted before member writes.
// Original assignments remain fixed throughout a transition, including retargets.
type PlacementPlanStatus struct {
	ID string `json:"id"`
	// Mode identifies the execution semantics of this accepted plan.
	// +optional
	// +kubebuilder:validation:Enum=Single;All;Split;SplitByCapacity
	Mode PlacementMode `json:"mode,omitempty"`
	// Winner commits the admitted Single home before race losers are removed.
	// Empty means the persisted Single admission race has not selected a home.
	// +optional
	Winner string `json:"winner,omitempty"`
	// SingleMove retains bounded replacement-race state until the old home and
	// losing probes have been removed. Winner remains the serving home.
	// +optional
	SingleMove *PlacementSingleMoveStatus `json:"singleMove,omitempty"`
	// AdoptionDigest fixes the initial intent while standing homes are being
	// inventoried. A changed intent cannot expand an incomplete inventory.
	// +optional
	AdoptionDigest string `json:"adoptionDigest,omitempty"`
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

// PlacementSingleMoveStatus separates replacement admission from serving handoff.
type PlacementSingleMoveStatus struct {
	// Selected records the admitted replacement while it becomes ready and routable.
	// +optional
	Selected string `json:"selected,omitempty"`
	// Cursor identifies the last nominated replacement for fair bounded retries.
	// +optional
	Cursor string `json:"cursor,omitempty"`
}

// CandidateAllocationStatus separates original, currently authorized, and desired
// floors. A zero current floor does not authorize deletion before traffic drains.
type CandidateAllocationStatus struct {
	ClusterUID types.UID `json:"clusterUID"`
	// InventoryPending prevents unknown standing resources from becoming free
	// migration capacity. It is cleared only by an identified member inventory.
	// +optional
	InventoryPending bool `json:"inventoryPending,omitempty"`
	// HomeInputsPending holds movement when current full-policy inputs cannot
	// be verified, while retaining the last accepted desired floors.
	// +optional
	HomeInputsPending bool `json:"homeInputsPending,omitempty"`
	// RaceCandidate authorizes cleanup of a Single race copy after another
	// candidate wins. A retained winner does not carry this permission.
	// +optional
	RaceCandidate bool `json:"raceCandidate,omitempty"`
	// ReplacementStartedAt persists the start of an authorized bounded probe.
	// The configured replacement timeout is evaluated against this instant.
	// +optional
	ReplacementStartedAt *metav1.MicroTime `json:"replacementStartedAt,omitempty"`
	// Matched distinguishes a desired full-policy home from an outgoing home,
	// including an affinity that matches every registration without terms.
	// +optional
	Matched bool `json:"matched,omitempty"`
	// CurrentHome retains the full policy authorized at a home through draining.
	// +optional
	CurrentHome *PlacementHomePolicy `json:"currentHome,omitempty"`
	// DesiredHome is present for every intended full-policy home.
	// +optional
	DesiredHome *PlacementHomePolicy `json:"desiredHome,omitempty"`
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

// PlacementHomePolicy records the full component floors of one retained home.
type PlacementHomePolicy struct {
	// InputDigest binds these resolved floors to the accepted source intent.
	InputDigest string `json:"inputDigest"`
	// +listType=map
	// +listMapKey=component
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=3
	ReplicaFloors []PlacementComponentFloor `json:"replicaFloors"`
}

// PlacementCapacitySample records the identified hardware and resolved demand
// used to normalize one member into nominal whole-replica capacity.
type PlacementCapacitySample struct {
	DemandFingerprint string `json:"demandFingerprint"`
	// DemandContract binds normalization to the member rendering authorized by
	// this allocation. Its fingerprint must equal DemandFingerprint.
	// +optional
	DemandContract *PlacementDemandContract `json:"demandContract,omitempty"`
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
	// Demand requires member rendering to match the accepted normalization
	// inputs before a component can project workloads. Absent for static plans.
	// +optional
	Demand *PlacementDemandContract `json:"demand,omitempty"`
	// ReplicaFloors binds the full per-home policy to the floors accepted by
	// placement. Runtime changes cannot expand a paused placement reservation.
	// +optional
	// +listType=map
	// +listMapKey=component
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=3
	ReplicaFloors []PlacementComponentFloor `json:"replicaFloors,omitempty"`
}

// PlacementComponentFloor records a resolved component's guaranteed count.
// Engine and Decoder use equal whole-replica floors; Router is independent.
// Zero permits steady scale-to-zero; movement requires positive floors.
type PlacementComponentFloor struct {
	// +kubebuilder:validation:Enum=engine;decoder;router
	Component ComponentType `json:"component"`
	// +kubebuilder:validation:Minimum=0
	Replicas int32 `json:"replicas"`
}

// PlacementDemandContract binds component rendering to normalized unit demand.
// Router is excluded because its replicas follow a per-home policy.
type PlacementDemandContract struct {
	// Fingerprint identifies the complete resolved resource/flavor demand.
	// +kubebuilder:validation:Pattern=`^[a-f0-9]{64}$`
	Fingerprint string `json:"fingerprint"`
	// Components includes exactly the declared Engine/Decoder components.
	// +listType=map
	// +listMapKey=component
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=2
	Components []PlacementComponentDemand `json:"components"`
}

// PlacementComponentDemand identifies one component's rendered replica shape.
type PlacementComponentDemand struct {
	// +kubebuilder:validation:Enum=engine;decoder
	Component ComponentType `json:"component"`
	// RenderingHash includes pod shapes, worker count, mode and RuntimeClasses.
	// +kubebuilder:validation:Pattern=`^[a-f0-9]{64}$`
	RenderingHash string `json:"renderingHash"`
}
