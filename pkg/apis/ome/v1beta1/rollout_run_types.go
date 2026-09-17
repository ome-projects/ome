// Package v1beta1 — rollout run status types.
//
// A rollout run is one traversal of the effective rollout plan for one target
// revision set. At run open the controller renders the effective plan (each
// group's inline progression or its referenced RolloutPolicy body), validates
// the composition, and pins it into status.rollout.activeRun in the same
// status write that carries the executor's step state — so the pinned plan
// and the step counters can never be observed out of sync. Executors consume
// the pinned plan for the duration of the run; spec and policy edits are
// inert mid-run (surfaced via the RolloutPlanDrift condition) and take effect
// at the next run open.
package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RolloutPlanSource records where a pinned group's progression came from.
// +kubebuilder:validation:Enum=Inline;Policy
type RolloutPlanSource string

const (
	// RolloutPlanSourceInline: the group's own inline progression (which
	// always outranks a coexisting policyRef — the preview mechanism).
	RolloutPlanSourceInline RolloutPlanSource = "Inline"
	// RolloutPlanSourcePolicy: rendered verbatim from the referenced
	// RolloutPolicy (or, on a derived ISVC, inflated from it at derive time).
	RolloutPlanSourcePolicy RolloutPlanSource = "Policy"
)

// RolloutRunOutcome is how a closed run ended.
// +kubebuilder:validation:Enum=Completed;RolledBack;Superseded
type RolloutRunOutcome string

const (
	// RolloutRunCompleted: every grouped member converged to the run's target
	// (or its group reached the Staged resting state under a partition).
	RolloutRunCompleted RolloutRunOutcome = "Completed"
	// RolloutRunRolledBack: the run ended in the rolled-back hold; the sticky
	// reject survives the run close.
	RolloutRunRolledBack RolloutRunOutcome = "RolledBack"
	// RolloutRunSuperseded: a retarget (or a repin to an empty plan) closed
	// the run before it converged; a fresh run opens against the new target.
	RolloutRunSuperseded RolloutRunOutcome = "Superseded"
)

// RolloutStatus is the run-model surface under status.rollout: the pinned
// active run, a bounded record of the previous run, and the always-current
// per-group resolution view (what WOULD pin now — the preview surface).
type RolloutStatus struct {
	// ActiveRun is the pinned plan the executors are consuming. Present iff a
	// run is open.
	// +optional
	ActiveRun *RolloutRun `json:"activeRun,omitempty"`

	// LastRun is the bounded record of the most recently closed run (digests
	// and outcome, never the full plan — post-close plan history is the
	// policy's git history).
	// +optional
	LastRun *RolloutRunRecord `json:"lastRun,omitempty"`

	// Groups is the always-current resolution view, written every reconcile
	// independent of runs: per group, which source would pin now and — when an
	// inline progression shadows a ref — what the shadowed policy would pin.
	// +optional
	// +listType=atomic
	Groups []RolloutGroupResolution `json:"groups,omitempty"`
}

// RolloutRun is one open rollout run: identity, targets, and the pinned plan.
type RolloutRun struct {
	// RunID uniquely names this run (stable for its lifetime; a retarget
	// closes the run and opens a new one with a new ID).
	RunID string `json:"runID"`

	// OpenedAt is when the run was pinned.
	OpenedAt metav1.Time `json:"openedAt"`

	// PinnedAt advances on every plan pin — the open, and each applied
	// repin. It is the run state's monotonic clock: concurrent status
	// writers keep whichever side is newer, so a reconcile working from a
	// stale cache can never roll a pinned plan back to a predecessor (a
	// repin is one-shot — its verb is consumed — so a rolled-back pin would
	// be unrecoverable, unlike every self-healing status field).
	PinnedAt metav1.Time `json:"pinnedAt"`

	// TargetRevisions records, per grouped Component, the revision hash this
	// run is rolling toward and the stable hash it can roll back to. A
	// Component's target changing mid-run is a retarget: the run closes
	// (Superseded) and a fresh run opens with a fresh render.
	// +optional
	// +listType=map
	// +listMapKey=component
	TargetRevisions []RolloutRunTarget `json:"targetRevisions,omitempty"`

	// Plan is the effective groups list, frozen for this run. Executors index
	// it — never the live spec — while the run is open.
	Plan RolloutRunPlan `json:"plan"`
}

// RolloutRunTarget is one grouped Component's pinned target revision.
type RolloutRunTarget struct {
	Component ComponentType `json:"component"`
	// Revision is the Component's target revision hash at run open.
	// +optional
	Revision string `json:"revision,omitempty"`
	// StableRevision is the Component's last promoted revision hash at run
	// open. It is preserved across retargets and is the exact rollback target.
	// +optional
	StableRevision string `json:"stableRevision,omitempty"`
}

// RolloutRunPlan is the frozen effective plan: the resolved groups in their
// pinned order. Group list-index identity (which keys the coordination
// engine's sticky state) indexes THIS list for the whole run, so mid-run
// group reorders in the spec cannot re-key ratio anchors or soak clocks.
type RolloutRunPlan struct {
	// +optional
	// +listType=atomic
	Groups []RolloutRunGroup `json:"groups,omitempty"`
}

// RolloutRunGroup is one pinned group: the resolved RolloutGroup the
// executors consume, plus the provenance of where its progression came from.
type RolloutRunGroup struct {
	// Source is where this group's progression came from.
	Source RolloutPlanSource `json:"source"`

	// PolicyRef identifies the policy when Source is Policy. On derived ISVCs
	// (multi-cluster inflation) it is recovered from the derive-time
	// provenance annotation so members report the same identity a
	// locally-resolved ref would.
	// +optional
	PolicyRef *RolloutPolicyRef `json:"policyRef,omitempty"`

	// PolicyGeneration is the referenced policy's generation at pin time.
	// +optional
	PolicyGeneration int64 `json:"policyGeneration,omitempty"`

	// PortableDigest is the pinned policy body's portable digest ("rp1:..."),
	// or — for inline groups — the digest of the inline progression body, so
	// drift detection works for both sources.
	// +optional
	PortableDigest string `json:"portableDigest,omitempty"`

	// Group is the resolved group: components plus exactly one inline
	// progression (a pinned group never carries a PolicyRef — the ref was
	// resolved at pin time).
	Group RolloutGroup `json:"group"`
}

// RolloutRunRecord is the bounded record of a closed run.
type RolloutRunRecord struct {
	Outcome RolloutRunOutcome `json:"outcome"`
	// +optional
	OpenedAt *metav1.Time `json:"openedAt,omitempty"`
	// +optional
	ClosedAt *metav1.Time `json:"closedAt,omitempty"`
	// TargetRevisions retains each Component's target and stable revision so a
	// rolled-back hold has an exact per-Component identity after ActiveRun is
	// dropped.
	// +optional
	// +listType=map
	// +listMapKey=component
	TargetRevisions []RolloutRunTarget `json:"targetRevisions,omitempty"`
	// Groups carries per-group provenance (groups may reference different
	// policies, so one digest per run would be lossy).
	// +optional
	// +listType=atomic
	Groups []RolloutRunProvenance `json:"groups,omitempty"`
}

// RolloutRunProvenance is the provenance subset of a pinned group, kept after
// the run closes.
type RolloutRunProvenance struct {
	Source RolloutPlanSource `json:"source"`
	// +optional
	PolicyRef *RolloutPolicyRef `json:"policyRef,omitempty"`
	// +optional
	PortableDigest string `json:"portableDigest,omitempty"`
}

// RolloutGroupResolution is the always-current resolution view for one spec
// group: which source would pin now, and the shadowed-policy preview when an
// inline progression outranks a ref.
type RolloutGroupResolution struct {
	// Index is the group's position in spec.rollout.groups[].
	Index int32 `json:"index"`

	// Source is the source a run opened now would pin for this group.
	Source RolloutPlanSource `json:"source"`

	// +optional
	PolicyRef *RolloutPolicyRef `json:"policyRef,omitempty"`

	// ObservedDigest is the portable digest of the progression body Source
	// resolves to right now (live render — compare against the active run's
	// pinned digest to see drift).
	// +optional
	ObservedDigest string `json:"observedDigest,omitempty"`

	// ShadowedPolicyRef is set when an inline progression outranks a ref: the
	// preview surface an operator diffs before deleting the inline block.
	// +optional
	ShadowedPolicyRef *ShadowedRolloutPolicyRef `json:"shadowedPolicyRef,omitempty"`
}

// ShadowedRolloutPolicyRef reports what an outranked policy ref would pin.
type ShadowedRolloutPolicyRef struct {
	Name string `json:"name"`
	// WouldPinDigest is the portable digest of the policy body that a run
	// would pin if the inline progression were removed.
	// +optional
	WouldPinDigest string `json:"wouldPinDigest,omitempty"`
}

// Condition types and reasons for the run model, reported on the
// InferenceService.
const (
	// RolloutPlanReadyCondition is False when the effective plan cannot be
	// resolved and the rollout is PARKED: the new revision is minted but no
	// update gate opens and no traffic moves — the old revision keeps serving.
	// Parking is deliberate fail-closed behavior: a group whose ref cannot
	// resolve must never silently execute the default progression, because
	// that would remove a canary gate without anyone asking.
	RolloutPlanReadyCondition = "RolloutPlanReady"

	RolloutPlanReasonPinned              = "Pinned"
	RolloutPlanReasonNoRun               = "NoActiveRun"
	RolloutPlanReasonPolicyNotFound      = "PolicyNotFound"
	RolloutPlanReasonPolicyNotReady      = "PolicyNotReady"
	RolloutPlanReasonProgressionMismatch = "ProgressionMismatch"
	RolloutPlanReasonPlanInvalid         = "PlanInvalid"
	RolloutPlanReasonProviderUnbound     = "ProviderUnbound"

	// RolloutPlanDriftCondition is True while the live render of the effective
	// plan differs from the pinned one: an edit landed mid-run and is inert
	// until the next run (or an explicit repin).
	RolloutPlanDriftCondition = "RolloutPlanDrift"

	RolloutPlanDriftReasonInSync             = "InSync"
	RolloutPlanDriftReasonPolicyNewerThanRun = "PolicyNewerThanRun"
	RolloutPlanDriftReasonSpecNewerThanRun   = "SpecNewerThanRun"
)

// RolloutRunActive reports whether a run is pinned — the executors' license
// to initialize or re-arm rollout state machines. Maintenance of terminal
// holds (done sentinel, rolled-back, failed) never needs it.
func RolloutRunActive(isvc *InferenceService) bool {
	return isvc != nil && isvc.Status.Rollout != nil && isvc.Status.Rollout.ActiveRun != nil
}

// AsRolloutSpec materializes the pinned plan as a RolloutSpec so existing
// resolution code (coordination.ResolveGroups, the canary executor) consumes
// it unchanged. live supplies the always-live PairingProtocol.
func (p *RolloutRunPlan) AsRolloutSpec(live *RolloutSpec) *RolloutSpec {
	if p == nil {
		return live
	}
	out := &RolloutSpec{Groups: make([]RolloutGroup, 0, len(p.Groups))}
	for i := range p.Groups {
		out.Groups = append(out.Groups, p.Groups[i].Group)
	}
	if live != nil {
		out.PairingProtocol = live.PairingProtocol
	}
	return out
}
