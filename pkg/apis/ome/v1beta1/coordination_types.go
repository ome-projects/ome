// Package v1beta1 — cross-Component rollout coordination types.
//
// The operator-facing surface for declaring how OMENative-managed
// Components within an InferenceService coordinate during a rollout is
// spec.rollout (see rollout_types.go). The RolloutCoordinationSpec and
// RolloutCoordinationGroup types below are superseded by spec.rollout
// and retained only for compatibility. The status and policy/pacing
// types remain in use.
package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// RolloutCoordinationSpec declares cross-Component rollout coupling
// for OMENative-managed Components. Each group lists the Components
// that roll together and the policy that governs their coordination.
// Components not listed in any group roll independently.
//
// Superseded by spec.rollout (RolloutSpec); retained for compatibility.
type RolloutCoordinationSpec struct {
	// Groups is the list of coordination groups. Each group binds a
	// set of Components under one policy. A Component may appear in
	// at most one group; webhook rejects multi-membership.
	// +optional
	// +listType=atomic
	Groups []RolloutCoordinationGroup `json:"groups,omitempty"`
}

// RolloutCoordinationGroup binds a set of Components under one
// coordination policy.
//
// Superseded by spec.rollout.groups[] (RolloutGroup); retained for
// compatibility.
type RolloutCoordinationGroup struct {
	// Components lists the Component names included in this group.
	// Valid values: "router", "engine", "decoder".
	// +kubebuilder:validation:MinItems=1
	// +listType=atomic
	Components []ComponentType `json:"components"`

	// Policy selects the coordination semantic for this group.
	// +kubebuilder:validation:Enum=BlueGreen;RollingUpdate;Independent;Sequential
	Policy CoordinationPolicy `json:"policy"`

	// Pacing controls how surge / drain pacing is computed within
	// the group. When omitted, defaults to per-Component pacing.
	// +optional
	Pacing *CoordinationPacing `json:"pacing,omitempty"`

	// Order has two meanings depending on Policy:
	//   - BlueGreen: pins the surge sequence within the single rollout
	//     unit (decoder surges first, then engine, etc.) but all
	//     Components traffic-shift together once every set is Ready.
	//     Optional; default no ordering (parallel surge).
	//   - Sequential: REQUIRED. Pins the order in which Components
	//     walk their full state machine. The next Component cannot
	//     enter Surging until the previous Component is back at Idle
	//     on the new revision.
	//   - Independent / RollingUpdate: ignored.
	// +optional
	// +listType=atomic
	Order []ComponentType `json:"order,omitempty"`

	// Soak is the duration the coordination layer waits AFTER a
	// Sequential Component reaches Idle on the new revision BEFORE
	// the next Component in Order begins its rollout. Lets operators
	// validate one Component under live production traffic before
	// risking the next. Only honored for Policy=Sequential.
	//
	// Default: 0 (no wait — the next Component starts immediately).
	// +optional
	Soak *metav1.Duration `json:"soak,omitempty"`
}

// CoordinationPolicy selects the cross-Component coordination semantic
// applied to a RolloutCoordinationGroup.
// +kubebuilder:validation:Enum=BlueGreen;RollingUpdate;Independent;Sequential
type CoordinationPolicy string

const (
	// CoordinationPolicyBlueGreen ties every Component in the group
	// into a single rollout unit: surge the full new set alongside the
	// old, wait for every new pod to be serving, then atomically flip
	// the routing Service selector from the old revision hash to the
	// new. If any Component fails to reach Ready, the entire group
	// enters Failed and the new set is torn down (no flip).
	CoordinationPolicyBlueGreen CoordinationPolicy = "BlueGreen"

	// CoordinationPolicyIndependent rolls each Component on its own. The
	// group declaration is documentation only; behavior is identical to
	// Components not listed in any group. Useful as an explicit assertion
	// that the operator has considered coordination and decided against it.
	CoordinationPolicyIndependent CoordinationPolicy = "Independent"

	// CoordinationPolicyRollingUpdate rolls Components in parallel
	// during the rollout window, tolerating mixed revisions (any vN
	// pod may talk to any vM pod within the group). Pacing knobs
	// (maxSurge, maxUnavailable, RatioBalanced) apply.
	CoordinationPolicyRollingUpdate CoordinationPolicy = "RollingUpdate"

	// CoordinationPolicySequential walks each Component's full state
	// machine in the strict Order declared on the group. The next
	// Component cannot enter Surging until the previous Component is
	// back at Idle on its new revision. Used for staged validation
	// under production traffic between Component rollouts.
	CoordinationPolicySequential CoordinationPolicy = "Sequential"
)

// CoordinationPacing controls how surge / drain pacing is computed
// within a coordination group.
type CoordinationPacing struct {
	// Type selects the pacing computation.
	//   - PerComponent: each Component's lifecycle.updateStrategy
	//     rollingUpdate applies independently. Group-wide MaxSurge
	//     still applies on top. Default.
	//   - RatioBalanced: pacing surges proportionally across all
	//     Components in the group, never letting the cross-Component
	//     ratio drift past the original.
	// +kubebuilder:validation:Enum=PerComponent;RatioBalanced
	// +optional
	Type CoordinationPacingType `json:"type,omitempty"`

	// MaxSurge is the maximum fraction (or absolute count) of pods
	// OMENative may add above the desired Component replica count
	// during a coordinated rollout. Applies group-wide.
	// Default: 25%.
	// +optional
	// +kubebuilder:validation:XIntOrString
	MaxSurge *intstr.IntOrString `json:"maxSurge,omitempty"`

	// MaxUnavailable is the maximum fraction (or absolute count) of
	// pods that may be not-Ready at once during the rollout. Applies
	// group-wide.
	// Default: 0.
	// +optional
	// +kubebuilder:validation:XIntOrString
	MaxUnavailable *intstr.IntOrString `json:"maxUnavailable,omitempty"`

	// RatioTolerancePercent bounds how far the components in a
	// RatioBalanced group may drift from their starting proportion during
	// a roll. The ratio is taken larger-pool : smaller-pool, so it is
	// direction-independent — prefill:decode and decode:prefill are the
	// same constraint — and is anchored to the counts observed when the
	// roll began. A surge OR drain is paused whenever a step would push
	// any component pair's live serving proportion outside
	// starting_ratio ± this percent, and resumes once the pools
	// rebalance. Example: a group that starts 2:1 with a value of 5 holds
	// the live proportion between 1.9:1 and 2.1:1. For three or more
	// components every pair is checked. When nil, the operator-configured
	// default (the coordination block of the operator's ConfigMap) applies
	// at resolution; unresolved (nil after resolution) means no drift
	// bound is enforced.
	// +optional
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	RatioTolerancePercent *int32 `json:"ratioTolerancePercent,omitempty"`
}

// CoordinationPacingType selects the surge-pacing computation for a
// RolloutCoordinationGroup.
// +kubebuilder:validation:Enum=PerComponent;RatioBalanced
type CoordinationPacingType string

const (
	// CoordinationPacingPerComponent applies per-Component rolling
	// update pacing independently for each Component in the group,
	// constrained only by the group-wide MaxSurge ceiling.
	CoordinationPacingPerComponent CoordinationPacingType = "PerComponent"

	// CoordinationPacingRatioBalanced surges Components in lockstep at
	// the cross-Component ratio observed at rollout start, refusing
	// any surge that would push the ratio outside the tolerance band.
	CoordinationPacingRatioBalanced CoordinationPacingType = "RatioBalanced"
)

// RolloutCoordinationStatus reports per-group coordination state for
// the InferenceService.
type RolloutCoordinationStatus struct {
	// Groups reports the observed state of each declared
	// rollout group, indexed by position in spec.rollout.groups[].
	// +optional
	// +listType=map
	// +listMapKey=name
	Groups []RolloutCoordinationGroupStatus `json:"groups,omitempty"`
}

// RolloutCoordinationGroupStatus reports the observed state of one
// coordination group.
type RolloutCoordinationGroupStatus struct {
	// Name uniquely identifies the group within the
	// RolloutCoordinationStatus. Auto-derived from the group index
	// (stringified) on first observation.
	Name string `json:"name"`

	// Components mirrors spec.rollout.groups[i].components
	// for operator readability.
	// +listType=atomic
	Components []ComponentType `json:"components,omitempty"`

	// Policy mirrors the spec policy.
	// +optional
	Policy CoordinationPolicy `json:"policy,omitempty"`

	// Order mirrors the spec order (Sequential / BlueGreen-ordered).
	// +optional
	// +listType=atomic
	Order []ComponentType `json:"order,omitempty"`

	// Phase reports the base coordination phase. Metric-stable enum.
	// +optional
	Phase CoordinationPhase `json:"phase,omitempty"`

	// CompositePhase is a human-readable composite of Phase plus the
	// active Component for Sequential / per-Component-aware policies
	// (e.g. "decoder.Surging", "Sequential.AwaitingNextComponent").
	// Operator-facing; metrics use Phase, not CompositePhase.
	// +optional
	CompositePhase string `json:"compositePhase,omitempty"`

	// CurrentComponent names the Component the group is actively
	// reconciling (Sequential policy). Empty otherwise.
	// +optional
	CurrentComponent ComponentType `json:"currentComponent,omitempty"`

	// PreviousComponent names the most recently completed Component in
	// a Sequential group's order. Empty when no Component has yet
	// reached Idle on the new revision.
	// +optional
	PreviousComponent ComponentType `json:"previousComponent,omitempty"`

	// MemberFinished records, for a Sequential group with a wait between
	// members, the member whose roll most recently read done and the
	// instant it first read so. The wait before the next member runs from
	// that instant on the group and on the replica side; the member losing
	// readiness afterwards, or its replica being recreated, does not move
	// it. Cleared when that member rolls again.
	// +optional
	MemberFinished *RolloutCoordinationMemberFinish `json:"memberFinished,omitempty"`

	// MemberNotReady names the finished Sequential member whose Ready
	// condition is False or absent while the next member waits on it: the
	// next member is held until that member is Ready again, and the wait
	// keeps running from MemberFinished meanwhile, so the next member starts
	// on the recovery when the wait has elapsed by then. Empty otherwise.
	// +optional
	MemberNotReady ComponentType `json:"memberNotReady,omitempty"`

	// ObservedSurge is the IntOrString surge target applied during the
	// current Surging / Waiting phase. Empty when the group is Idle.
	// +optional
	ObservedSurge *intstr.IntOrString `json:"observedSurge,omitempty"`

	// ObservedRatio reports cross-Component ratio snapshots when the
	// group uses RatioBalanced pacing. Nil for PerComponent pacing.
	// +optional
	ObservedRatio *RolloutCoordinationRatio `json:"observedRatio,omitempty"`

	// LastTransitionTime is when Phase last changed.
	// +optional
	LastTransitionTime *metav1.Time `json:"lastTransitionTime,omitempty"`

	// MixedPairing reports that the group's Components serve distinct
	// revisions at once, which a RollingUpdate group permits while it
	// rolls; a BlueGreen or Sequential group never sets it. It holds for
	// as long as the picture does. The event MixedPairingObserved is
	// recorded on the pass that sets it, not on every pass it holds.
	// +optional
	MixedPairing bool `json:"mixedPairing,omitempty"`

	// SurgeHeldByRatio reports that RatioBalanced pacing refused the
	// group's next surge on this pass to keep the cross-Component ratio.
	// It holds for as long as the refusal does. The event RatioSkewRejected
	// is recorded on the pass that sets it, not on every pass it holds.
	// +optional
	SurgeHeldByRatio bool `json:"surgeHeldByRatio,omitempty"`

	// Message is a short operator-facing explanation of the current
	// Phase. Set on Failed / RollingBack / Paused phases.
	// +optional
	Message string `json:"message,omitempty"`
}

// RolloutCoordinationMemberFinish is the instant a Sequential member's
// roll first read done, the clock the wait before the next member runs
// from.
type RolloutCoordinationMemberFinish struct {
	// Component names the member.
	Component ComponentType `json:"component"`

	// At is the instant the member first read done.
	At metav1.Time `json:"at"`
}

// CoordinationPhase is the metric-stable enum for a coordination
// group's base reconcile phase.
// +kubebuilder:validation:Enum=Idle;Surging;Waiting;Shifting;Draining;ScalingDown;Staged;Failed;RollingBack;Paused
type CoordinationPhase string

const (
	// CoordinationPhaseIdle indicates no rollout is in flight on this
	// group. Steady state.
	CoordinationPhaseIdle CoordinationPhase = "Idle"

	// CoordinationPhaseSurging indicates the group is creating
	// new-revision pods up to the surge target.
	CoordinationPhaseSurging CoordinationPhase = "Surging"

	// CoordinationPhaseWaiting indicates the group is waiting for
	// new-revision pods to reach Ready.
	CoordinationPhaseWaiting CoordinationPhase = "Waiting"

	// CoordinationPhaseShifting indicates the group is writing
	// Status.Components.<c>.Traffic[] with the new weights; the
	// HTTPRoute builder picks up the change downstream.
	CoordinationPhaseShifting CoordinationPhase = "Shifting"

	// CoordinationPhaseDraining indicates the group is waiting
	// scaleDownDelaySeconds for in-flight requests to drain on
	// old-revision pods before scale-down.
	CoordinationPhaseDraining CoordinationPhase = "Draining"

	// CoordinationPhaseScalingDown indicates the group is deleting
	// old-revision pods per the pacing budget.
	CoordinationPhaseScalingDown CoordinationPhase = "ScalingDown"

	// CoordinationPhaseStaged indicates the group has converged to a
	// partitioned target and is intentionally holding old-revision pods
	// (static rollingUpdate.partition). A terminal RESTING phase, distinct
	// from Idle ("no rollout / single revision"): the rollout is complete
	// for the configured partition.
	CoordinationPhaseStaged CoordinationPhase = "Staged"

	// CoordinationPhaseFailed indicates a permanent error during the
	// rollout (Ready timeout, persistent reconcile error, etc.).
	// Operator action required.
	CoordinationPhaseFailed CoordinationPhase = "Failed"

	// CoordinationPhaseRollingBack indicates the group is reverting
	// an in-flight surge to restore the previous revision.
	CoordinationPhaseRollingBack CoordinationPhase = "RollingBack"

	// CoordinationPhasePaused indicates the group is held at the
	// current phase pending operator action (paused annotation or a
	// step-boundary pause).
	CoordinationPhasePaused CoordinationPhase = "Paused"
)

// RolloutCoordinationRatio is the cross-Component ratio snapshot used
// by RatioBalanced pacing.
type RolloutCoordinationRatio struct {
	// Original is the reference the ratio rule measures against: each
	// Component's desired replica count as its spec asks for it now. It
	// follows the spec, so a resize mid-roll or while idle moves it to
	// the new counts. The keys are ComponentType strings.
	// +optional
	Original map[ComponentType]int32 `json:"original,omitempty"`

	// Current reports the live replica counts per Component.
	// +optional
	Current map[ComponentType]int32 `json:"current,omitempty"`

	// NewPods reports the new-revision pod counts per Component.
	// +optional
	NewPods map[ComponentType]int32 `json:"newPods,omitempty"`
}

// CompositePhaseSequentialAwaiting is the well-known composite phase
// string for a Sequential group sitting in its inter-Component soak
// window. Idle at the metric layer; awaiting the operator's next
// spec bump.
const CompositePhaseSequentialAwaiting = "Sequential.AwaitingNextComponent"

// CompositePhaseSequentialFailed is the well-known composite phase
// string for a Sequential group blocked downstream by a previous
// Component's Failed phase.
const CompositePhaseSequentialFailed = "Sequential.Failed"

// RolloutCoordinationReady is the InferenceServiceStatus condition
// type for coordination readiness. True when every group is Idle.
const RolloutCoordinationReady = "RolloutCoordinationReady"

// CoordinationAdvisory is the InferenceServiceStatus condition type for
// the "multiple OMENative Components but no coordination group" hint.
// True (active) records that the advisory applies; it is the "already
// advised" memory so the ConsiderCoordinationGroup event fires once on
// transition, not every reconcile. Not a dependent of the Ready
// conditionSet, so it never affects overall readiness.
const CoordinationAdvisory = "CoordinationAdvisory"

// TrafficEndpointsReady is the InferenceServiceStatus condition type for
// the data-plane side of the traffic split. False while a revision has
// serving pods that are not yet Ready endpoints of its routed Service, with
// a message naming the revision and the counts; the split withholds that
// revision's share until the endpoints controller lists them. True once
// every serving pod is listed. Not a dependent of the Ready conditionSet.
const TrafficEndpointsReady = "TrafficEndpointsReady"
