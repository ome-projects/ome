package coordination

import (
	"fmt"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	workloadops "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/ops"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// RatioState carries the cross-Component replica counts the
// RatioBalanced pacing math reasons about. Snapshotted at rollout
// start (Original) and refreshed every reconcile (Current, NewPods,
// Serving) while the group is in Surging / Waiting.
type RatioState struct {
	// Original maps Component → desired replica count at the moment
	// the rollout began. Used as the ratio anchor; the tolerance band
	// is computed against this.
	Original map[v1beta1.ComponentType]int32

	// Current maps Component → total live replica count (any
	// revision) as of this reconcile.
	Current map[v1beta1.ComponentType]int32

	// Desired maps Component to its authoritative desired Instance count from
	// InferenceReplica spec. A nil spec.replicas resolves to the API default of
	// one. Recovery never adds serving capacity beyond this shape.
	Desired map[v1beta1.ComponentType]int32

	// NewPods maps Component → number of new-revision pods (i.e.,
	// pods on the target ControllerRevision) as of this reconcile. The
	// serving-capacity band projects against Serving (live traffic
	// capacity), but the surge-path new-revision lockstep
	// (outpacesPeerProgress) reads NewPods: under SurgeThenDrain serving
	// holds at the original N for every Component, so only rollout
	// progress can pace the leading Component.
	NewPods map[v1beta1.ComponentType]int32

	// Serving maps Component → number of pods CURRENTLY in the load
	// balancer's rotation (serving gate True AND containers Ready),
	// counting both revisions together. RatioBalanced math projects
	// against this — operators care about live traffic capacity, not
	// rollout progress fractions.
	Serving map[v1beta1.ComponentType]int32

	// RecoveryEligible marks Components whose authoritative desired shape is
	// positive but whose live serving capacity is below that shape. The ratio
	// gate sets this only after observing every member's IR status, so a nil map
	// keeps callers on the strict ratio path.
	RecoveryEligible map[v1beta1.ComponentType]bool

	// InFlightSurge maps Component to active SurgeThenDrain operations, including
	// operations persisted in IR status and operations started in this reconcile
	// wake-up. Recovery pacing admits at most one outstanding capacity-restoring
	// surge per Component.
	InFlightSurge map[v1beta1.ComponentType]int32

	// InFlightUnavail maps Component → number of Instances currently
	// taken OUT of serving by an in-flight drain/recreate/in-place
	// operation, counted from Operation.Step (NOT the cache-derived
	// Serving). The dispatcher stamps Operation.Step synchronously, so
	// this is FRESH even when Serving (pod-cache derived) still
	// over-reports during a recreate burst. The discrete-granularity
	// tiebreaker reads it to bound in-flight drains to a single pod
	// across reconcile wake-ups. Optional: a nil map reads as 0 (no
	// in-flight), which is the correct default for the surge path and
	// for unit callers that don't populate it.
	InFlightUnavail map[v1beta1.ComponentType]int32
}

// RatioDecision is the answer for "may we surge new-revision pods for
// `component`?" produced by RatioBalanced pacing.
type RatioDecision struct {
	// AllowedSurgeDelta is the maximum number of new-revision pods we
	// may add to component this reconcile. Zero when no surge is
	// allowed; negative is normalized to zero.
	AllowedSurgeDelta int32

	// SkewRejected is true when the previous attempted surge for
	// component was refused to avoid drifting the cross-Component
	// ratio outside the tolerance band. Drives the
	// `ratio_skew_total` counter and the RatioSkewRejected event.
	SkewRejected bool

	// Reason is the operator-visible explanation.
	Reason string
}

// EvaluateSurge returns the per-Component delta budget that respects
// the cross-Component live-serving ratio. It is a pure function: feed
// it the pacing block, the snapshot state, the Component being
// considered, and the proposed delta — get back what the controller
// may apply.
//
// Delta semantic: +N means "the action will INCREASE serving by N
// pods for `component`" (surge a new ready pod). Negative deltas
// (drain) go through the same band check via CheckRatioGate with
// projection = current - 1.
//
// Math: the original prefill:decode (or any pair, largest original on
// top) ratio is anchored at rollout start. After the proposed action,
// the live serving ratio for every component pair must stay within
// original_ratio * (1 ± tolerance). Pairwise so 3+-way topologies
// (engine + decoder + router) are constrained against every pair.
func EvaluateSurge(pacing v1beta1.CoordinationPacing, state RatioState, component v1beta1.ComponentType, proposedDelta int32) RatioDecision {
	if proposedDelta == 0 {
		return RatioDecision{AllowedSurgeDelta: 0, Reason: "no surge requested"}
	}
	if pacing.Type != v1beta1.CoordinationPacingRatioBalanced {
		return RatioDecision{AllowedSurgeDelta: proposedDelta, Reason: "PerComponent pacing"}
	}
	if len(state.Original) <= 1 {
		// Single-Component group — ratio enforcement is trivially
		// satisfied (no peer to skew against).
		return RatioDecision{AllowedSurgeDelta: proposedDelta, Reason: "single-component group"}
	}
	if pacing.RatioTolerancePercent == nil {
		// No tolerance resolved: the group omitted it and the operator
		// configured no default. There is no band to enforce, so the guard
		// admits the step rather than inventing a bound.
		return RatioDecision{AllowedSurgeDelta: proposedDelta, Reason: "no ratio tolerance resolved; drift bound disabled"}
	}
	band := float64(*pacing.RatioTolerancePercent) / 100.0

	// Project against live Serving counts. Falls back to NewPods for
	// backward compat with callers that haven't populated Serving yet.
	baseline := state.Serving
	if baseline == nil {
		baseline = state.NewPods
	}
	// Drain (proposedDelta < 0): the gate is asked "may I pull one more
	// pod from rotation right now?" The projection is single-step — no
	// reduction loop because a partial drain isn't a thing the
	// dispatcher can act on (the unit of work is one pod).
	if proposedDelta < 0 {
		projected := projectedServing(baseline, component, proposedDelta)
		if respectsBand(state.Original, projected, band) {
			return RatioDecision{
				AllowedSurgeDelta: proposedDelta,
				Reason:            "drain within tolerance",
			}
		}
		// Discrete-granularity tiebreaker (rounding-error only). When the
		// tolerance band is narrower than one whole pod at the current
		// operating point, NO single -1 drain can satisfy the strict band,
		// and a drain-first strategy (RecreatePod / in-place) deadlocks
		// permanently — e.g. a symmetric 4:4 pair at tol 25%: draining
		// either side projects max/min = 4/3 = 1.333 > 1.25.
		//
		// Allow exactly ONE in-flight pod through, but only when the skew it
		// introduces is rounding-error-sized:
		//   (a) NO pod of the drained Component is already out of rotation:
		//       - no in-flight drain/recreate op (state.InFlightUnavail is
		//         operation-tracked, so it is FRESH even when the cache-derived
		//         Serving lags during a recreate burst); AND
		//       - the baseline is at full original serving (CheckRatio nets
		//         THIS-wake-up drains out of the baseline, so the second pod in
		//         one wake-up sees a reduced baseline and is refused).
		//       Together these bound in-flight to one pod ACROSS wake-ups and
		//       restrict the tiebreaker to the genuine "can't even start"
		//       deadlock — a Component mid-roll isn't deadlocked, so the strict
		//       band edge stands (see the 30:10 boundary test).
		//   (b) the pair is currently balanced (baseline in band);
		//   (c) the drained Component keeps >=1 pod serving — the mass-drain
		//       guard (zero-serving side) stays intact; and
		//   (d) the projection respects a band relaxed by one extra
		//       tolerance width (2x band). A large asymmetric skew (e.g.
		//       4:2 decoder->1 = 4:1) exceeds 2x band and stays an honest
		//       deadlock the operator resolves by widening tolerance.
		if state.InFlightUnavail[component] == 0 &&
			baseline[component] >= state.Original[component] &&
			respectsBand(state.Original, baseline, band) &&
			projected[component] >= 1 &&
			respectsBand(state.Original, projected, band*2) {
			return RatioDecision{
				AllowedSurgeDelta: proposedDelta,
				Reason:            "drain allowed by discrete-granularity tiebreaker (1 pod, rounding-error overshoot)",
			}
		}
		return RatioDecision{
			AllowedSurgeDelta: 0,
			SkewRejected:      true,
			Reason:            "drain would skew cross-Component serving ratio past tolerance",
		}
	}
	// New-revision lockstep. The serving-capacity band below is invariant
	// under SurgeThenDrain — a surge rotates its replacement IN before the
	// source drains OUT, so live serving stays at the original N:N for every
	// Component the whole roll. That makes the capacity band (and its
	// tiebreaker) authorize every surge and the gate never holds the leading
	// Component: engine finishes its entire roll while decoder still lags,
	// and their per-Instance readiness dips fall out of alignment, so the
	// live ready-capacity ratio sits skewed (engine 2 : decoder 1) for the
	// whole gap. Hold the Component whose new-revision rollout is already
	// running ahead of a peer's: refuse to START another surge when this
	// Component's rollout fraction already outpaces a peer's beyond the
	// band. A Component that is level with (or behind) every peer always
	// proceeds, so the pair advances in lockstep and never deadlocks.
	if outpacesPeerProgress(state.Original, state.NewPods, component, band) {
		return recoverySurgeFallback(state, baseline, component, RatioDecision{
			AllowedSurgeDelta: 0,
			SkewRejected:      true,
			Reason:            "surge would outpace peer new-revision rollout past tolerance",
		})
	}
	for delta := proposedDelta; delta > 0; delta-- {
		projected := projectedServing(baseline, component, delta)
		if respectsBand(state.Original, projected, band) {
			return RatioDecision{
				AllowedSurgeDelta: delta,
				Reason:            "within tolerance",
			}
		}
	}
	// Surge-side tiebreaker. When the band is narrower than one whole pod at
	// the current operating point, NO single +1 surge satisfies the strict
	// band and a surge-first roll deadlocks (e.g. symmetric 3:3 at tol 25%:
	// surging either side to 4 is 4:3 = 1.333 > 1.25, so BOTH deny and neither
	// goes first). A surge only ADDS a pod to `component` — it raises
	// component's serving, never lowers a peer's — so it can NEVER starve a
	// peer no matter what ratio it transiently shows, and SurgeThenDrain drains
	// it back to N. So allow ONE surge whenever:
	//   (a) the surging Component is at full original serving (a genuine
	//       start-of-roll deadlock, not a mid-roll skew — mirrors the drain
	//       baseline>=original guard; rejects the already-skewed / mid-roll
	//       and zero-serving cases the strict band must keep gating); AND
	//   (b) the pair is currently balanced (baseline in band — rejects the
	//       already-skewed / zero-peer cases).
	// The overshoot magnitude is deliberately NOT capped. The prior 2x-band
	// cap false-deadlocked the UNAVOIDABLE minimal surge at small N / tight
	// band — N=1 (surge 1->2 = 2:1) and tol 10% at N=4 (5:4 = 1.25 > 1.2) —
	// permanently wedging a SurgeThenDrain roll that can only proceed by
	// surging. Since a surge can't starve a peer (the rationale above) and is
	// transient, capping its appearance contradicts that rationale; MaxSurge
	// (CheckSurge) already bounds how many surges run at once.
	if baseline[component] >= state.Original[component] &&
		respectsBand(state.Original, baseline, band) {
		return RatioDecision{
			AllowedSurgeDelta: 1,
			Reason:            "surge allowed (minimal transient +1 on a balanced baseline; a surge cannot starve a peer)",
		}
	}
	return recoverySurgeFallback(state, baseline, component, RatioDecision{
		AllowedSurgeDelta: 0,
		SkewRejected:      true,
		Reason:            "surge would skew cross-Component serving ratio past tolerance",
	})
}

// recoverySurgeFallback admits one capacity-restoring step only after the
// ordinary positive-surge path rejects, preserving all existing approvals.
func recoverySurgeFallback(state RatioState, serving map[v1beta1.ComponentType]int32, component v1beta1.ComponentType, rejected RatioDecision) RatioDecision {
	if !state.RecoveryEligible[component] || state.InFlightSurge[component] > 0 ||
		!recoverySurgeAllowed(state, serving, component) {
		return rejected
	}
	reason := "capacity recovery advances a lagging Component"
	if serving[component] == 0 {
		reason = "capacity recovery bootstrap for zero-serving Component"
	}
	return RatioDecision{
		AllowedSurgeDelta: 1,
		Reason:            reason,
	}
}

// recoverySurgeAllowed admits a step when the Component's serving/desired
// fraction is not ahead of any deficient peer. Cross multiplication is exact.
func recoverySurgeAllowed(state RatioState, serving map[v1beta1.ComponentType]int32, component v1beta1.ComponentType) bool {
	currentC := state.Current[component]
	desiredC := state.Desired[component]
	servingC := serving[component]
	if currentC <= 0 || desiredC <= 0 || servingC < 0 || servingC >= desiredC || servingC+1 > desiredC {
		return false
	}
	for c, desired := range state.Desired {
		if desired <= 0 || state.Current[c] <= 0 || serving[c] < 0 {
			return false
		}
	}
	for peer, desiredP := range state.Desired {
		if peer == component || !state.RecoveryEligible[peer] {
			continue
		}
		left := int64(servingC) * int64(desiredP)
		right := int64(serving[peer]) * int64(desiredC)
		if left > right {
			return false
		}
	}
	return true
}

// outpacesPeerProgress reports whether `component` is ALREADY rolling
// ahead of a peer by more than the tolerance band — the signal to hold it
// from starting yet another new-revision surge. It is the lockstep guard
// the serving-capacity band cannot supply: under SurgeThenDrain live
// serving holds at the original N for every Component (a surge rotates its
// replacement IN before the source drains OUT), so the capacity math never
// registers a skew and never paces the leading Component — engine outruns
// decoder and the live ready-capacity ratio drifts off band for the whole
// gap.
//
// Progress is measured as the rollout FRACTION new[c]/original[c] so the
// guard is ratio-correct for asymmetric pairs (a 4:2 group rolls in step
// when both are 50% rolled = 2 new : 1 new, not 2 new : 2 new). The
// tolerance band is read as an allowed lead in fraction units, mirroring
// the serving band's "± band of the starting ratio" semantic.
//
// The comparison is on CURRENT progress (before this surge), not projected:
// surging advances in whole-Instance steps, so a Component leaving a tie
// unavoidably takes a transient one-Instance lead. Gating on the projected
// post-surge lead would deny that first step and deadlock a roll that can
// only proceed by surging (the same wedge the serving-side tiebreaker
// exists to avoid). Gating on the CURRENT lead instead lets a level-or-
// behind Component take the minimal step, but holds a Component that is
// already out front. Properties:
//   - No deadlock: a Component level with or behind every still-rolling
//     peer always proceeds, so the slowest peer is never blocked.
//   - No false hold at the finish: a peer that has fully rolled does not
//     constrain, so the trailing Component's final surges aren't blocked.
//   - First surge always allowed: at 0 new : 0 new neither side leads.
//
// A Component alone in its group (no peer with positive original) is never
// outpacing anyone — returns false. A nil newPods map reads as all-zero
// (no progress yet), the correct default for the first reconcile and for
// unit callers that don't populate it.
func outpacesPeerProgress(original, newPods map[v1beta1.ComponentType]int32, component v1beta1.ComponentType, band float64) bool {
	if band < 0 {
		band = 0
	}
	origC := original[component]
	if origC <= 0 {
		return false
	}
	fracC := float64(newPods[component]) / float64(origC)
	for peer, origP := range original {
		if peer == component || origP <= 0 {
			continue
		}
		// A peer that has finished rolling does not pace this Component.
		if newPods[peer] >= origP {
			continue
		}
		peerFrac := float64(newPods[peer]) / float64(origP)
		if fracC-peerFrac > band {
			return true
		}
	}
	return false
}

// projectedServing clones the baseline serving map with component += delta.
// Negative values are floored at zero — a drain can't take serving
// below zero, but the band check will reject that projection
// (zero-serving denominator → undefined ratio → out of band).
func projectedServing(in map[v1beta1.ComponentType]int32, component v1beta1.ComponentType, delta int32) map[v1beta1.ComponentType]int32 {
	out := make(map[v1beta1.ComponentType]int32, len(in)+1)
	for k, v := range in {
		out[k] = v
	}
	out[component] += delta
	if out[component] < 0 {
		out[component] = 0
	}
	return out
}

// respectsBand reports whether the projected live serving counts, taken
// pairwise as larger/smaller ratios, stay within `band` of the original
// pairwise ratio at rollout start.
//
// Semantic: the "RatioBalanced" name promises the live cross-Component
// capacity ratio (e.g., prefill:decode) stays in band, not per-Component
// rollout progress drift. Convention: for each pair, larger original on
// top so the ratio is always >= 1 and bands are symmetric multiplicatively.
//
// For each pair (a, b) with originals OA, OB (assume OA >= OB):
//
//	originalRatio = OA / OB
//	bandLow       = originalRatio * (1 - band)
//	bandHigh      = originalRatio * (1 + band)
//	projRatio     = max(proj[a], proj[b]) / min(proj[a], proj[b])
//	require: bandLow <= projRatio <= bandHigh
//
// A component projected to zero serving makes the ratio undefined /
// infinite → out of band → reject. This is what blocks the mass-drain
// failure mode at the gate.
//
// Pairwise so 3+-way topologies (engine + decoder + router) are
// constrained against every pair, not just the largest.
func respectsBand(original, projected map[v1beta1.ComponentType]int32, band float64) bool {
	if band < 0 {
		band = 0
	}
	type entry struct {
		comp v1beta1.ComponentType
		orig int32
		proj int32
	}
	items := make([]entry, 0, len(original))
	for c, orig := range original {
		if orig <= 0 {
			// Components with zero original replicas can't constrain ratio.
			continue
		}
		items = append(items, entry{comp: c, orig: orig, proj: projected[c]})
	}
	if len(items) <= 1 {
		return true
	}
	for i := 0; i < len(items); i++ {
		for j := i + 1; j < len(items); j++ {
			a, b := items[i], items[j]
			// Larger-original on top → ratio is always >= 1.
			if a.orig < b.orig {
				a, b = b, a
			}
			origRatio := float64(a.orig) / float64(b.orig)
			bandLow := origRatio * (1 - band)
			bandHigh := origRatio * (1 + band)
			// Project pairwise live serving counts (already after the
			// proposed action — see EvaluateSurge).
			pa, pb := a.proj, b.proj
			if pa < pb {
				pa, pb = pb, pa
			}
			if pb <= 0 {
				// Smaller side dropped to zero serving → undefined ratio.
				// This is the mass-drain failure mode the gate must block.
				return false
			}
			projRatio := float64(pa) / float64(pb)
			if projRatio < bandLow || projRatio > bandHigh {
				return false
			}
		}
	}
	return true
}

// SnapshotOriginal records the desired replica counts at rollout
// start so RatioBalanced has a stable anchor. Caller passes the
// per-Component effective MinReplicas; outputs are int32 because
// ResolveGroup / status writers expect that shape.
//
// Components with MinReplicas == 0 are recorded as 1 — a zero-replica
// Component can't anchor a ratio, and treating zero as one yields the
// same "any surge allowed" behavior as the single-Component path.
func SnapshotOriginal(replicas map[v1beta1.ComponentType]int32) map[v1beta1.ComponentType]int32 {
	out := make(map[v1beta1.ComponentType]int32, len(replicas))
	for c, r := range replicas {
		if r <= 0 {
			r = 1
		}
		out[c] = r
	}
	return out
}

// unavailableInFlight counts a Component's Instances currently taken OUT
// of serving rotation by an in-flight operation (Drain for recreate,
// InPlace for in-place patch). Surge lifecycle steps / GangSurgeTarget ADD a
// pod (or a whole replacement gang) rather than remove one, so they are
// excluded. Mirrors workload.CurrentUnavailableInFlight on the v1beta1
// status shape.
//
// This is OPERATION-tracked (the dispatcher stamps Operation.Step
// synchronously when it starts the op), so it is fresh even when the
// pod-cache-derived ServingReplicas still over-reports during a recreate
// burst — which is what lets the discrete-granularity tiebreaker bound
// in-flight drains to a single pod across reconcile wake-ups.
func unavailableInFlight(statuses []v1beta1.OMENativeInstanceStatus) int32 {
	var n int32
	for _, s := range statuses {
		if s.Operation == nil {
			continue
		}
		switch s.Operation.Step {
		case workloadops.UpdateStepSurge, workloadops.UpdateStepSurgeDrain,
			workloadops.UpdateStepSurgeDrainSettle, workloadtypes.UpdateStepGangSurgeTarget:
			// surge (single-pod or gang replacement) adds capacity, it
			// doesn't remove a pod from rotation
		default:
			n++
		}
	}
	return n
}

// surgeInFlight counts source Instances with an active SurgeThenDrain
// operation. Gang replacements carry a target marker in addition to their
// source operation, so counting source lifecycle steps records each surge once.
func surgeInFlight(statuses []v1beta1.OMENativeInstanceStatus) int32 {
	var n int32
	for _, s := range statuses {
		if s.Operation == nil {
			continue
		}
		switch s.Operation.Step {
		case workloadops.UpdateStepSurge, workloadops.UpdateStepSurgeDrain, workloadops.UpdateStepSurgeDrainSettle:
			n++
		}
	}
	return n
}

// servingSurgePeakInFlight counts a Component's in-flight gang-surge TARGET
// Instances whose replacement gang is serving WHILE its paired source gang is
// also still serving — the transient PEAK that inflates ServingReplicas above
// the steady N during a multi-node SurgeThenDrain roll. A single-pod surge
// flips serving atomically within one reconcile (old leaves rotation the
// instant the new joins), so its peak is never observed across a status write;
// a gang's replacement lingers serving beside the not-yet-drained source.
// Netting this out makes the ratio gate see a gang Component at its STEADY
// serving — the same view it has of a single-pod Component — so the surge
// headroom is not mistaken for durable capacity a drain may consume.
//
// The peak exists ONLY while BOTH gangs serve. Once the source is drained
// (gangSurgeUpdate flips it serving=False before deletion) ServingReplicas
// already collapses to the steady N, even though the target keeps its
// GangSurgeTarget marker until promote. Netting the target out in that window
// would manufacture a phantom N-1 TROUGH and swing the ratio the other way —
// so only count a target whose source is still in rotation.
func servingSurgePeakInFlight(statuses []v1beta1.OMENativeInstanceStatus) int32 {
	// Target indices whose surge SOURCE gang is still fully serving — the
	// signal that the peak is real. A source carries Op.SurgeIndex pointing
	// at its target; once drained its ServingPodCount drops below PodCount.
	sourceStillServing := make(map[int32]bool)
	for _, s := range statuses {
		if s.Operation == nil || s.Operation.SurgeIndex == nil {
			continue
		}
		if s.PodCount > 0 && s.ServingPodCount >= s.PodCount {
			sourceStillServing[*s.Operation.SurgeIndex] = true
		}
	}
	var n int32
	for _, s := range statuses {
		if s.Operation == nil || s.Operation.Step != workloadtypes.UpdateStepGangSurgeTarget {
			continue
		}
		// Counted in ServingReplicas only once the whole gang is serving,
		// and only a TRUE peak (its source still serving beside it).
		if s.PodCount > 0 && s.ServingPodCount >= s.PodCount && sourceStillServing[s.Index] {
			n++
		}
	}
	return n
}

// CheckRatio is the consumer side of RatioBalanced pacing. The
// dispatcher calls ResolveGateContext once per per-Instance tick and
// threads the resulting context into each gate via CheckRatio /
// CheckSurge / CheckUnavailability / CheckSequential, paying the
// resolve prelude exactly once per tick instead of once per gate.
//
// It asks: "given the live cross-Component pod distribution, may this
// Component bump one more pod to the new revision without skewing the
// ratio past tolerance?"
//
// Returns allowed=true when:
//   - The Component is not in any RatioBalanced coord group,
//   - The component is alone in its group (no peer to skew against),
//   - The group has no ObservedRatio snapshot yet,
//   - OR the projected serving capacity stays within the group's
//     RatioTolerancePercent band against every peer Component.
//
// Returns allowed=false when EvaluateSurge rejects the +1 surge. Caller
// requeues; the peer Component will have caught up by the next reconcile.
//
// The ratio anchor comes from ISVC coordination status. Live per-Component
// shape, serving capacity, rollout progress, and operations come from the
// authoritative InferenceReplica objects through the supplied reader.
//
// inFlightSurge is the number of surges the dispatcher has already started in
// this wake-up for the caller's Component. inFlightUnavail is the number of
// pods it has already pulled from rotation. IR status reflects the prior pass,
// so both fresh tallies participate in the decision.
func (ctx GateContext) CheckRatio(inFlightSurge, inFlightUnavail, projDelta int32) (allowed bool, reason string) {
	if ctx.ShortCircuit {
		return true, ctx.ShortReason
	}
	group := ctx.Group
	isvc := ctx.ISVC
	component := ctx.Component
	if group.Pacing.Type != v1beta1.CoordinationPacingRatioBalanced {
		return true, "not RatioBalanced pacing"
	}
	if len(group.Components) <= 1 {
		return true, "single-component group"
	}

	// Original is read from the recorded ObservedRatio. Live shape,
	// serving, progress, and operations are read from authoritative IR status.
	state := RatioState{
		Original:         make(map[v1beta1.ComponentType]int32, len(group.Components)),
		Current:          make(map[v1beta1.ComponentType]int32, len(group.Components)),
		Desired:          make(map[v1beta1.ComponentType]int32, len(group.Components)),
		NewPods:          make(map[v1beta1.ComponentType]int32, len(group.Components)),
		Serving:          make(map[v1beta1.ComponentType]int32, len(group.Components)),
		RecoveryEligible: make(map[v1beta1.ComponentType]bool, len(group.Components)),
		InFlightSurge:    make(map[v1beta1.ComponentType]int32, len(group.Components)),
		InFlightUnavail:  make(map[v1beta1.ComponentType]int32, len(group.Components)),
	}
	var groupStatus *v1beta1.RolloutCoordinationGroupStatus
	if isvc.Status.RolloutCoordination != nil {
		for i := range isvc.Status.RolloutCoordination.Groups {
			gs := &isvc.Status.RolloutCoordination.Groups[i]
			if gs.Name == group.Name {
				groupStatus = gs
				break
			}
		}
	}
	hasObservedRatio := groupStatus != nil && groupStatus.ObservedRatio != nil && len(groupStatus.ObservedRatio.Original) > 0
	if !hasObservedRatio {
		return true, "no ObservedRatio snapshot yet (first reconcile)"
	}
	recoveryObservable := true
	for _, c := range group.Components {
		orig := groupStatus.ObservedRatio.Original[c]
		if orig <= 0 {
			orig = 1
		}
		state.Original[c] = orig

		// Read from authoritative IR status. A read error is NOT "no
		// observation yet": deny and let the caller requeue, otherwise a
		// transient apiserver blip feeds a phantom zero-serving peer into
		// EvaluateSurge and disables the ratio band exactly when the
		// cluster is unhealthy.
		ir, _, err := irprojector.DecodedComponentIR(ctx.Ctx, ctx.Reads, isvc.Namespace, isvc.Name, c)
		if err != nil {
			return false, fmt.Sprintf("cannot read %s IR status, failing closed: %v", c, err)
		}
		if ir == nil {
			recoveryObservable = false
			continue
		}
		summary := &ir.Status
		desired := int32(1)
		if ir.Spec.Replicas != nil {
			desired = *ir.Spec.Replicas
		}
		if summary.Replicas <= 0 || desired <= 0 {
			recoveryObservable = false
		}
		state.Current[c] = summary.Replicas
		state.Desired[c] = desired
		state.NewPods[c] = summary.UpdatedReplicas
		// Serving is what the band check projects against — live
		// traffic capacity, not rollout progress.
		// Net out the transient gang-surge PEAK (replacement gang
		// serving beside the not-yet-drained source) so a multi-node
		// Component is gated at its STEADY serving, exactly like a
		// single-pod Component whose surge flip is atomic. Without this
		// a gang's N+1 peak is mistaken for durable capacity and the
		// gate authorizes a drain that settles the ratio out of band.
		serving := summary.ServingReplicas - servingSurgePeakInFlight(summary.InstanceStatuses)
		if serving < 0 {
			serving = 0
		}
		state.Serving[c] = serving
		state.RecoveryEligible[c] = desired > 0 && serving < desired
		state.InFlightSurge[c] = surgeInFlight(summary.InstanceStatuses)
		// Operation-tracked in-flight drains (fresh; see RatioState doc).
		state.InFlightUnavail[c] = unavailableInFlight(summary.InstanceStatuses)
	}
	if !recoveryObservable {
		clear(state.RecoveryEligible)
	}
	// Account for pods this wake-up has already started pulling out of
	// rotation but whose status_aggregate write hasn't landed yet. Only
	// the caller's Component carries in-flight here; peer Components'
	// in-flight is implicit in their own dispatchers' separate runs and
	// will be reflected in their next status write.
	if inFlightUnavail > 0 {
		state.Serving[component] -= inFlightUnavail
		if state.Serving[component] < 0 {
			state.Serving[component] = 0
		}
	}
	if inFlightSurge > 0 {
		state.InFlightSurge[component] += inFlightSurge
	}

	// projDelta is the projected per-pod transient capacity change for THIS
	// update step, set by the strategy-aware caller:
	//   - SurgeThenDrain → +1: a new pod is created and goes Ready BEFORE
	//     the old one drains, so the transient is +1 capacity.
	//   - recreate / drain-first → -1: the old pod leaves rotation first,
	//     so the transient is -1 capacity.
	//
	// Getting this sign right is load-bearing. A hardcoded -1 for a
	// SurgeThenDrain roll mis-models the surge as a capacity loss: on a
	// symmetric ratio (e.g. 4:4, tol 25% → band [0.75, 1.25]) a single -1
	// projects max/min = 4/3 = 1.333 > 1.25, so BOTH members are denied at
	// the start and neither can go first → permanent deadlock. With +1 the
	// same roll projects 5/4 = 1.25 (in band) and proceeds.
	decision := EvaluateSurge(group.Pacing, state, component, projDelta)
	if decision.AllowedSurgeDelta != 0 {
		return true, decision.Reason
	}
	return false, decision.Reason
}

// CheckUnavailability is the consumer side of MaxUnavailable pacing.
// The OMENative ops dispatcher calls it before kicking off a disruptive
// operation (Update recreate, in-place drain, Restart) to ask: "given
// the live pod observation PLUS the in-flight count from this wake-up,
// may this Component take one more pod offline without exceeding the
// group's MaxUnavailable budget?" See GateContext / CheckRatio for the
// pre-resolve pattern the dispatcher uses to pay the prelude once per
// per-Instance tick.
//
// inFlightDelta is the number of pods the dispatcher has ALREADY
// flipped to NotServing in this wake-up. Status counters from
// isvc.Status reflect the prior reconcile pass — they don't see
// in-wake-up writes — so the dispatcher must thread the running count
// in or every gate decision in one wake-up shares the same stale
// snapshot answer (the original mass-outage bug).
//
// Returns allowed=true when:
//   - The Component is not in any coord group,
//   - The component has no observed status yet (first reconcile),
//   - OR (currentUnavailable + inFlightDelta + 1) stays within
//     MaxUnavailableBudget for the Component.
//
// Counts pods serving traffic (ServingReplicas), NOT pods with merely
// running containers (ReadyReplicas). The two diverge during in-place
// updates: kubelet still reports ContainersReady while the controller
// has flipped the serving gate to False; using ReadyReplicas would
// mislead the gate into letting every pod get pulled from traffic at
// once (the original mass-outage bug at scale).
//
// Honest semantic: with MaxUnavailable=0 and an update strategy that
// requires drain-first (in-place, recreate without surge), this gate
// will deadlock rollouts. SurgeThenDrain side-steps the gate by
// creating new pods within the surge budget instead of taking old
// ones offline.
func (ctx GateContext) CheckUnavailability(inFlightDelta int32) (allowed bool, reason string) {
	if ctx.ShortCircuit {
		return true, ctx.ShortReason
	}

	// Read from authoritative IR status. Only a genuinely missing IR is
	// "no observation yet" (allow); a read error fails closed so a
	// transient apiserver blip can't disable the unavailability budget
	// and authorize a mass drain.
	summary, err := irprojector.ComponentIRStatus(ctx.Ctx, ctx.Reads, ctx.ISVC.Namespace, ctx.ISVC.Name, ctx.Component)
	if err != nil {
		return false, fmt.Sprintf("cannot read %s IR status, failing closed: %v", ctx.Component, err)
	}

	if summary == nil {
		return true, "no observation yet"
	}

	desired := summary.Replicas
	if desired <= 0 {
		desired = 1
	}
	// ServingReplicas (in-load-balancer count) NOT ReadyReplicas
	// (containers-running count) — in-place drain flips serving=False
	// before kubelet sees a container restart, so ReadyReplicas would
	// still report all pods Ready while half are actually out of rotation.
	currentUnavailable := desired - summary.ServingReplicas
	if currentUnavailable < 0 {
		currentUnavailable = 0
	}
	if inFlightDelta < 0 {
		inFlightDelta = 0
	}
	budget := MaxUnavailableBudget(ctx.Group.Pacing, desired)
	projected := currentUnavailable + inFlightDelta + 1
	if projected > budget {
		return false, fmt.Sprintf("unavailable budget %d exhausted (current %d, in-flight %d, would become %d)",
			budget, currentUnavailable, inFlightDelta, projected)
	}
	return true, "within unavailable budget"
}

// CheckSurge is the surge-side dual of CheckUnavailability.
// SurgeThenDrain never takes pods OFFLINE (so the unavailability gate
// is irrelevant) but it temporarily adds an extra pod per Instance
// (alive = desired + N where N is the number of in-flight surges).
// This gate enforces the group's MaxSurge budget across the Component.
// See GateContext / CheckRatio for the pre-resolve pattern the
// dispatcher uses to pay the prelude once per per-Instance tick.
//
// The dispatcher calls this only on `startingFresh` for instances
// whose UpdateStrategy is SurgeThenDrain (other strategies go through
// CheckUnavailability). Mirrors the inFlightDelta pattern from the
// other gates: status counters can't see in-wake-up writes, so the
// dispatcher threads its running tally to avoid the stale-snapshot
// trap.
//
// Returns allowed=true when:
//   - The Component is not in any coord group,
//   - The component has no observed status yet (first reconcile),
//   - OR (currentInFlightSurge + inFlightDelta + 1) stays within
//     MaxSurgeBudget for the Component.
//
// currentInFlightSurge = count of Instances whose Operation.Step is
// a surge lifecycle step — those are the Instances that
// already have an extra pod alive from a prior wake-up.
func (ctx GateContext) CheckSurge(inFlightDelta int32) (allowed bool, reason string) {
	if ctx.ShortCircuit {
		return true, ctx.ShortReason
	}

	// Read from authoritative IR status. Only a genuinely missing IR is
	// "no observation yet" (allow); a read error fails closed so a
	// transient apiserver blip can't erase the in-flight surge count and
	// blow the surge budget.
	summary, err := irprojector.DecodedComponentIRStatus(ctx.Ctx, ctx.Reads, ctx.ISVC.Namespace, ctx.ISVC.Name, ctx.Component)
	if err != nil {
		return false, fmt.Sprintf("cannot read %s IR status, failing closed: %v", ctx.Component, err)
	}

	if summary == nil {
		return true, "no observation yet"
	}

	desired := summary.Replicas
	if desired <= 0 {
		desired = 1
	}
	currentInFlight := surgeInFlight(summary.InstanceStatuses)
	if inFlightDelta < 0 {
		inFlightDelta = 0
	}
	budget := MaxSurgeBudget(ctx.Group.Pacing, desired)
	projected := currentInFlight + inFlightDelta + 1
	if projected > budget {
		return false, fmt.Sprintf("surge budget %d exhausted (current %d, in-flight %d, would become %d)",
			budget, currentInFlight, inFlightDelta, projected)
	}
	return true, "within surge budget"
}
