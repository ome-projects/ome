// Package policy defines the contract every Alfred policy implements: a
// pure, side-effect-free function of the ClusterSnapshot that returns ranked
// Candidates (OEP-0008 §The engine). A policy holds no client and emits no
// Event, metric, or ConfigMap entry — the engine gates scheduling before
// routing executable Candidates to the Arbiter and advisories to the Reporter.
package policy

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/alfred/config"
	"sigs.k8s.io/ome/pkg/alfred/scheduling"
	"sigs.k8s.io/ome/pkg/alfred/snapshot"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// Candidate reasons (event-facing; the dispatcher maps them to the wire
// contract's lowercase reason values).
const (
	ReasonFragmentation     = "Fragmentation"
	ReasonNodeUnhealthy     = "NodeUnhealthy"
	ReasonNodeMaintenance   = "NodeMaintenance"
	ReasonRemediationSignal = "RemediationSignal"
)

// Advisory reasons: why an emitted Candidate is not executable. Advisory
// candidates surface through the Reporter only — they never enter arbitration
// and never consume migration budget.
const (
	// AdvisoryNoSurgeHeadroom: the surge-shaped replacement footprint fits
	// on no feasible target while the source still holds its GPUs.
	AdvisoryNoSurgeHeadroom = "NoSurgeHeadroom"
	// AdvisoryVolumePinned: an RWO/RWOP PVC pins the workload to its node;
	// no migration mechanism can move it.
	AdvisoryVolumePinned = "VolumePinned"
	// AdvisoryLWSMigrationUnsupported: LWS tears down the whole group on
	// pod restart with no surge protection; the safe fix (move the
	// workload to OMENative) is the operator's.
	AdvisoryLWSMigrationUnsupported = "LWSMigrationUnsupported"
	// AdvisoryRawDeploymentMigrationUnsupported: Alpha has no truthful
	// RawDeployment executor; every otherwise observable Raw Instance is
	// surfaced to the operator without entering arbitration.
	AdvisoryRawDeploymentMigrationUnsupported = "RawDeploymentMigrationUnsupported"
	// AdvisoryOMENativeUnavailable: no OMENative executor exists on this
	// cluster, so the migration verb has no consumer.
	AdvisoryOMENativeUnavailable = "OMENativeUnavailable"
	// AdvisoryOMENativeObservationInvalid: the checked IR/Pod join is
	// missing, stale, or structurally inconsistent.
	AdvisoryOMENativeObservationInvalid = "OMENativeObservationInvalid"
	// AdvisoryOMENativeStateIneligible: the checked OMENative state is not
	// fully steady or the workload is already busy with a transition.
	AdvisoryOMENativeStateIneligible = "OMENativeStateIneligible"
	// AdvisoryNonExecutableObservedFragmentation: fragmentation is visible
	// to the operator but cannot authorize a positive executable move.
	AdvisoryNonExecutableObservedFragmentation = "NonExecutableObservedFragmentation"
	// AdvisoryMigrationSurfaceDisabled: the component's execution surface
	// is switched off in alfred-config.
	AdvisoryMigrationSurfaceDisabled = "MigrationSurfaceDisabled"
	// AdvisoryModelUnresolved: model availability could not be resolved
	// (see ModelAvailability.ResolveError); the policy treats the model as
	// having no feasible target and surfaces the reason.
	AdvisoryModelUnresolved = "ModelUnresolved"
)

// ComponentWideInstance marks a Candidate that addresses a whole component
// rather than one Instance (advisories such as VolumePinned or the LWS
// recommendation). Component-wide candidates are never dispatched.
const ComponentWideInstance int32 = -1

// NodeRemediation is a complete observation of a node's desired remediation
// state. Physical OME GPU occupancy is retained even when workload identity
// cannot be resolved, so an unjoined Pod cannot make a node appear drained.
type NodeRemediation struct {
	Node                   string
	NodeUID                types.UID
	ObservedAt             time.Time
	Health                 snapshot.NodeHealthObservation
	Maintenance            snapshot.NodeMaintenanceObservation
	Workloads              []string
	OMEGPUOccupantsPresent bool
}

// Candidate is a single proposed action — "migrate Component X's Instance Y
// off Node Z" — with explicit, cross-policy-comparable benefit and cost so
// the Arbiter can rank on a common axis instead of trusting each policy's
// self-assessment.
type Candidate struct {
	// Policy is the emitting policy's Name().
	Policy string

	Workload  types.NamespacedName
	Component v1beta1.ComponentType
	// Instance is the addressed Instance index, or ComponentWideInstance.
	Instance int32
	// Mode is the component's resolved deployment mode, which determines
	// the execution surface.
	Mode constants.DeploymentModeType

	// Reason is the event-facing cause (Fragmentation, NodeUnhealthy, ...).
	Reason string
	// Remediation is set only on node markers, never workload move findings.
	Remediation *NodeRemediation

	// FromNode is an actual member node selected by the policy. Evacuation
	// prefers an unhealthy member, then a member requesting maintenance.
	FromNode string
	// HintTargetNodes is the bounded, ranked policy-supplied target set for
	// operator-facing reporting. The scheduler still makes the final pod-level
	// placement decision.
	HintTargetNodes []string
	// PlacementTargetNodes is an internal exhaustive, ranked target set for
	// replaying a policy's successful atomic placement during arbitration. It
	// is not part of operator-facing reports. Empty means the Arbiter falls
	// back to HintTargetNodes for compatibility with pluggable policies.
	PlacementTargetNodes []string

	// Executable=false marks an advisory finding; AdvisoryReason says why.
	Executable     bool
	AdvisoryReason string
	// Scheduling is added by the engine after policy evaluation. Profile
	// selection is preliminary routing, not a placement feasibility result.
	Scheduling *SchedulingDiagnostics

	// SurgeShaped records the simulated execution shape. Defragmentation
	// has one executable Alpha shape: OMENative place-then-free surge.
	SurgeShaped bool
	// FootprintGPUs is the instance's GPU footprint. For a surge-shaped
	// move this much headroom must exist while the source still holds its
	// GPUs; it is also the ranking tie-break (smaller moves first).
	FootprintGPUs int64

	// Benefit is the expected improvement (F_observed_before minus
	// F_observed_after on the simulated post-move state).
	Benefit float64
	// Cost is the disruption risk keyed off the migration mode.
	Cost float64
	// Score ranks candidates within their priority class. Defrag uses
	// benefit-minus-cost; evacuation uses the workload's numeric priority.
	Score float64
	// Emergency marks a candidate whose move unblocks a pending pod older
	// than emergencyPendingAgeMinutes.
	Emergency bool
}

// SchedulingDiagnostics records bounded scheduling outcomes separately from
// an advisory's original cause. The engine never reports backend payloads.
type SchedulingDiagnostics struct {
	SchedulerName    string `json:"schedulerName,omitempty"`
	Backend          string `json:"backend,omitempty"`
	SchedulerVersion string `json:"schedulerVersion,omitempty"`
	ConfigurationID  string `json:"configurationID,omitempty"`
	Status           string `json:"status"`
	Reason           string `json:"reason"`
	// Prediction provenance and placements are advisory, never arbiter hints.
	SnapshotID   string                 `json:"snapshotID,omitempty"`
	SnapshotTime *metav1.Time           `json:"snapshotTime,omitempty"`
	Placements   []scheduling.Placement `json:"placements,omitempty"`
}

// Policy is a pluggable decision module: a pure function of the snapshot.
type Policy interface {
	Name() string
	Evaluate(snap *snapshot.ClusterSnapshot, cfg *config.Config) []Candidate
}
