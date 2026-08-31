// Package policy defines the contract every Alfred policy implements: a
// pure, side-effect-free function of the ClusterSnapshot that returns ranked
// Candidates (OEP-0008 §The engine). A policy holds no client and emits no
// Event, metric, or ConfigMap entry — the engine routes its output: executable
// Candidates enter the Arbiter, advisory ones go straight to the Reporter.
package policy

import (
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/alfred/config"
	"sigs.k8s.io/ome/pkg/alfred/snapshot"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// Candidate reasons (event-facing; the dispatcher maps them to the wire
// contract's lowercase reason values).
const (
	ReasonFragmentation     = "Fragmentation"
	ReasonNodeUnhealthy     = "NodeUnhealthy"
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

	// FromNode is the node the move vacates (for a multi-node instance,
	// the node holding the largest share of its GPUs).
	FromNode string
	// HintTargetNodes is the ranked, advisory placement preference (top N;
	// the scheduler makes the final call).
	HintTargetNodes []string

	// Executable=false marks an advisory finding; AdvisoryReason says why.
	Executable     bool
	AdvisoryReason string

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
	// Score = Benefit - CostWeight*Cost, emergency-boosted when the move
	// unblocks an over-age pending pod.
	Score float64
	// Emergency marks a candidate whose move unblocks a pending pod older
	// than emergencyPendingAgeMinutes.
	Emergency bool
}

// Policy is a pluggable decision module: a pure function of the snapshot.
type Policy interface {
	Name() string
	Evaluate(snap *snapshot.ClusterSnapshot, cfg *config.Config) []Candidate
}
