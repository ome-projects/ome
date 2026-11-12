package coordination

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	workloadops "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/ops"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// RatioState carries the cross-Component replica counts the
// RatioBalanced pacing math reasons about. Original is the reference the
// ISVC pass records (and rewrites when a member's desired count changes);
// Current, NewPods and Serving are refreshed every reconcile while the
// group is in Surging / Waiting.
type RatioState struct {
	// Original maps Component → the reference count the ratio band measures
	// against: the Instance count the Component's spec asks for now, as the
	// ISVC pass last recorded it. It follows the spec, so after an operator
	// or an autoscaler resizes a side the band keeps the ratio the operator
	// asked for, not the shape the roll began with.
	Original map[v1beta1.ComponentType]int32

	// Current maps Component → total live replica count (any
	// revision) as of this reconcile.
	Current map[v1beta1.ComponentType]int32

	// Desired maps Component to its authoritative desired Instance count from
	// InferenceReplica spec. A nil spec.replicas resolves to the API default of
	// one. Recovery never adds serving capacity beyond this shape.
	Desired map[v1beta1.ComponentType]int32

	// NewPods maps Component → number of Instances that have landed on the
	// target ControllerRevision (the replica's updatedReplicas) as of this
	// reconcile. The serving-capacity band projects against Serving (live
	// traffic capacity), but the new-revision lockstep (outpacesPeerProgress)
	// reads NewPods, and OpenAttempts for the Component it paces: a surge
	// rotates its replacement in before the source drains, and a drain-first
	// roll with a budget of one has its replacement back before the next
	// drain, so serving holds at the original N for every Component under
	// every strategy and only rollout progress can pace the leading Component.
	NewPods map[v1beta1.ComponentType]int32

	// OpenAttempts maps Component → number of Instances whose move to the
	// target has started and not landed: the rows carrying an open update
	// attempt toward it. A start is written on its row before the next
	// start is consulted, so the rows carry the starts of this pass too and
	// the in-flight counts the stale counters need are not added here. The
	// lockstep counts them as the Component's own progress beside NewPods,
	// so a start is decided on the picture after every move under way; a
	// peer is credited only with what has landed. A nil map reads as 0.
	OpenAttempts map[v1beta1.ComponentType]int32

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

	// Creating maps Component → number of Instances whose row carries a
	// Create attempt: new Instances a scale-up is still bringing up. They
	// serve nothing yet and have lost nothing, so the lockstep's recovery
	// branch does not count them as capacity to refill. Optional: a nil map
	// reads as 0.
	Creating map[v1beta1.ComponentType]int32

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

	// HeldStillPeer is the pairing peer the pairing gates hold still for
	// the start under decision: its open attempts are surges whose old pods
	// the drain-time gate keeps, or every start it could open is held, so
	// it moves only once this Component has. The lockstep does not pace a
	// Component by such a peer, as the batch step does not wait on one;
	// the serving band still reads the peer's capacity. Empty when no
	// peer is held still, which is every start outside a pairing-protocol
	// change.
	HeldStillPeer v1beta1.ComponentType
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

	// OutsideTolerance is true when the admitted step leaves the serving
	// ratio outside the tolerance because no single step of the Component
	// can stay inside it at the group's shape; the band admits the one step
	// rather than holding the roll for good, and the caller says so on the
	// service.
	OutsideTolerance bool
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
// projection = current - 1. Both directions apply the new-revision
// lockstep first: a Component already rolled further than a still-rolling
// peer by more than the band waits, whatever the step does to capacity.
//
// Math: the reference prefill:decode (or any pair, largest reference on
// top) ratio is the desired counts the spec asks for now. After the
// proposed action, the live serving ratio for every component pair must
// stay within reference_ratio * (1 ± tolerance). Pairwise so 3+-way
// topologies (engine + decoder + router) are constrained against every pair.
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
		// New-revision lockstep, the same rule the surge path applies below.
		// A drain that starts a replacement moves one more Instance to the
		// new revision, and the serving band cannot see that progress: with
		// a budget of one each drain projects N-1:N, inside any band wider
		// than a pod, and the replacement is back before the next drain, so
		// the band alone lets the fast Component finish its roll while its
		// peer lags. A Component whose drain would carry it past its
		// allowance ahead of a peer waits; the lagging one proceeds, so the
		// pair never waits on itself.
		if outpacesPeerProgress(state.lockstepReference(), state.NewPods, state.OpenAttempts, component, *pacing.RatioTolerancePercent) {
			return RatioDecision{
				AllowedSurgeDelta: 0,
				SkewRejected:      true,
				Reason:            "drain would outpace peer new-revision rollout past tolerance",
			}
		}
		projected := projectedServing(baseline, component, proposedDelta)
		if respectsBand(state.Original, projected, band) {
			return RatioDecision{
				AllowedSurgeDelta: proposedDelta,
				Reason:            "drain within tolerance",
			}
		}
		// One pod at a time when the strict band refuses every single drain.
		// The band can be narrower than one whole pod at the fleet's shape:
		// a symmetric 4:4 pair at tolerance 25 projects 4/3 = 1.333 > 1.25
		// whichever side drains, and 8 prefill beside 2 decode at tolerance
		// 20 read one decode pod out as 8:1, 100 points off 8:2. A drain-
		// first side that is never admitted holds the whole roll for good,
		// so the band admits exactly one pod of the side, under three bounds:
		//   (a) no pod of the side is already out of rotation: no drain or
		//       recreate operation on its rows (operation-tracked, so fresh
		//       when the cache-derived Serving lags a recreate burst), and
		//       the baseline at the side's full reference (CheckRatio nets
		//       this pass's drains out of the baseline, so the second pod of
		//       one pass sees a reduced baseline and is refused). Together
		//       these bound the side to one pod out across passes and keep
		//       the allowance off a side mid-roll, which is not held for
		//       good (see the 30:10 boundary test);
		//   (b) the fleet is balanced (the baseline in band), so the step
		//       starts from the ratio the reference asks for; and
		//   (c) the side keeps a pod serving, unless its reference is one: a
		//       side of one has no step that keeps a pod serving, and its one
		//       drain is the drain-first step the operator chose for it.
		// A projection within twice the tolerance is the rounding-error
		// overshoot every drain-first roll under a narrow band takes. Past
		// that, the admitted step leaves the serving ratio outside the
		// tolerance for its length: the decision says so with the numbers
		// and the gate raises RatioStepOutsideTolerance on the service. The
		// lockstep above still paces the side against its peer, so it never
		// runs ahead by more than the tolerance and one step.
		oneStepOpen := state.InFlightUnavail[component] == 0 &&
			baseline[component] >= state.Original[component] &&
			respectsBand(state.Original, baseline, band) &&
			(projected[component] >= 1 || state.Original[component] == 1)
		switch {
		case oneStepOpen && projected[component] >= 1 && respectsBand(state.Original, projected, band*2):
			return RatioDecision{
				AllowedSurgeDelta: proposedDelta,
				Reason:            "drain allowed by discrete-granularity tiebreaker (1 pod, rounding-error overshoot)",
			}
		case oneStepOpen:
			return RatioDecision{
				AllowedSurgeDelta: proposedDelta,
				OutsideTolerance:  true,
				Reason:            oneStepOutsideToleranceReason(component, projected[component], state.Original, max(*pacing.RatioTolerancePercent, 0)),
			}
		}
		return RatioDecision{
			AllowedSurgeDelta: 0,
			SkewRejected:      true,
			Reason:            "drain would skew cross-Component serving ratio past tolerance",
		}
	}
	// New-revision lockstep, the same rule the drain branch applies above.
	// The serving-capacity band below is invariant under SurgeThenDrain — a
	// surge rotates its replacement IN before the source drains OUT, so live
	// serving stays at the original N:N for every Component the whole roll.
	// That makes the capacity band (and its tiebreaker) authorize every surge
	// and the gate never holds the leading Component: engine finishes its
	// entire roll while decoder still lags, and their per-Instance readiness
	// dips fall out of alignment, so the live ready-capacity ratio sits
	// skewed (engine 2 : decoder 1) for the whole gap. Hold the Component
	// whose new-revision rollout would run ahead of a peer's: refuse to
	// START another surge when it would carry this Component's progress
	// past its allowance ahead of a peer. A Component that is level with
	// (or behind) every peer always proceeds, so the pair advances in
	// lockstep and never waits on itself.
	if outpacesPeerProgress(state.lockstepReference(), state.NewPods, state.OpenAttempts, component, *pacing.RatioTolerancePercent) {
		held := RatioDecision{
			AllowedSurgeDelta: 0,
			SkewRejected:      true,
			Reason:            "surge would outpace peer new-revision rollout past tolerance",
		}
		// Capacity recovery refills pods a Component has lost since the roll
		// began; it never buys a lead over a still-rolling peer. A Component
		// serving at least its anchor count has lost nothing, so a larger
		// desired count alone (a scale-up mid-roll) leaves the hold in place.
		if !servingBelowAnchor(state, baseline, component) {
			return held
		}
		return recoverySurgeFallback(state, baseline, component, held)
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

// oneStepOutsideToleranceReason words the band's admission of a drain no
// single step could keep inside the tolerance: the side's serving count
// after the drain over its reference, the tolerance, and the reference
// ratio the group holds.
func oneStepOutsideToleranceReason(component v1beta1.ComponentType, servingAfter int32, reference map[v1beta1.ComponentType]int32, tolerance int32) string {
	return fmt.Sprintf("%s %d of %d serving after the drain; no single step fits tolerance %d at %s; admitting one",
		component, servingAfter, reference[component], tolerance, referenceRatioText(reference))
}

// referenceRatioText renders the reference counts as one ratio, engine
// first, then decoder, then router, then any other member by name, so the
// same group always reads the same way ("8:2").
func referenceRatioText(reference map[v1beta1.ComponentType]int32) string {
	rank := func(c v1beta1.ComponentType) int {
		switch c {
		case v1beta1.EngineComponent:
			return 0
		case v1beta1.DecoderComponent:
			return 1
		case v1beta1.RouterComponent:
			return 2
		}
		return 3
	}
	comps := make([]v1beta1.ComponentType, 0, len(reference))
	for c := range reference {
		comps = append(comps, c)
	}
	sort.Slice(comps, func(i, j int) bool {
		if rank(comps[i]) != rank(comps[j]) {
			return rank(comps[i]) < rank(comps[j])
		}
		return comps[i] < comps[j]
	})
	parts := make([]string, 0, len(comps))
	for _, c := range comps {
		parts = append(parts, strconv.Itoa(int(reference[c])))
	}
	return strings.Join(parts, ":")
}

// recoverySurgeFallback admits one capacity-restoring step only after the
// ordinary positive-surge path rejects, preserving all existing approvals.
// It refills a Component that serves fewer pods than its desired shape so a
// roll cannot wedge below desired; the new-revision lockstep is consulted
// before it and yields only to a Component that is also below its anchor
// (servingBelowAnchor).
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

// servingBelowAnchor reports whether component has lost serving capacity:
// fewer pods serve than the Instances it has rows for that are not still
// being created, bounded by its reference count. The lockstep assumes
// serving holds at the reference for every Component (see
// RatioState.NewPods), so a Component below it is short of capacity the
// recovery fallback may refill, one surge at a time, even while the
// lockstep holds it. The reference follows the spec, so after a scale-up
// it names Instances that do not exist yet or are still being created;
// those have lost nothing, and a side serving every Instance it had keeps
// the hold until they serve. A side whose rows are not observed (no
// Current count) is read against the reference alone.
func servingBelowAnchor(state RatioState, serving map[v1beta1.ComponentType]int32, component v1beta1.ComponentType) bool {
	have := state.Original[component]
	if settled := state.Current[component] - state.Creating[component]; state.Current[component] > 0 && settled < have {
		have = settled
	}
	return serving[component] < have
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

// outpacesPeerProgress reports whether one more new-revision step of
// `component` would carry it ahead of a still-rolling peer by more than its
// allowance — the signal to hold it from starting that step, whatever the
// step does to serving capacity: a surge, the drain that starts a
// replacement, or an in-place patch of the same pod. It is the lockstep
// guard the serving-capacity band cannot supply: live serving holds at the
// original N for every Component under every strategy (a surge rotates its
// replacement IN before the source drains OUT; a drain-first roll has its
// replacement back before the next drain; an in-place patch returns the
// same pod), so the capacity math never registers a skew and never paces
// the leading Component — engine outruns decoder and the live
// ready-capacity ratio drifts off band for the whole gap.
//
// Progress is measured as the rollout FRACTION new[c]/reference[c], the
// reference being the Instance count the Component's spec asks for now
// (RatioState.progressReference), so the guard is ratio-correct for
// asymmetric pairs (a 4:2 group rolls in step when both are 50% rolled =
// 2 new : 1 new, not 2 new : 2 new) and a side resized mid-roll is measured
// against the count it rolls toward. A peer whose new-revision Instances
// reach its current desired count has finished and paces nobody, so a side
// resized below its start count can finish.
//
// The gate projects the step it is about to admit: the Component's progress
// after the step is the Instances that have landed (newPods), the attempts
// open on its rows toward the target, the starts of this pass among them
// (open), and the step itself; a peer is credited only with what has
// landed. The lead that projection stands past the peer's share, in the
// Component's own Instances, may not exceed its allowance
// (lockstepAllowance): the tolerance of its own count rounded up to a whole
// Instance, at least one, since a roll advances in whole Instances.
// Properties:
//   - No hold with no way out: a Component level with or behind every
//     still-rolling peer always proceeds, so the slowest peer is never
//     blocked.
//   - No false hold at the finish: a peer that has fully rolled does not
//     constrain, so the trailing Component's final steps aren't blocked.
//   - First step always allowed: at 0 new : 0 new neither side leads.
//   - Never past the allowance by the gate's own doing: a pass admits
//     starts up to the allowance ahead of the peer and no more, however
//     wide the budget and however far the status counters trail the rows.
//
// A Component alone in its group (no peer with a positive reference) is
// never outpacing anyone — returns false. Nil newPods and open maps read
// as all-zero (no progress yet), the correct default for the first
// reconcile and for unit callers that don't populate them.
func outpacesPeerProgress(reference, newPods, open map[v1beta1.ComponentType]int32, component v1beta1.ComponentType, tolerancePercent int32) bool {
	refC := reference[component]
	if refC <= 0 {
		return false
	}
	afterStep := int64(newPods[component]) + int64(open[component]) + 1
	allowance := lockstepAllowance(tolerancePercent, refC)
	for peer, refP := range reference {
		if peer == component || refP <= 0 {
			continue
		}
		// A peer whose new-revision Instances reach its current desired
		// count has finished rolling and does not pace this Component.
		if newPods[peer] >= refP {
			continue
		}
		// The lead in the Component's Instances is afterStep less the peer's
		// share scaled to the Component's count; both sides are multiplied
		// by the peer's count so the comparison stays in integers.
		lead := afterStep*int64(refP) - int64(newPods[peer])*int64(refC)
		if lead > allowance*int64(refP) {
			return true
		}
	}
	return false
}

// lockstepAllowance is how many Instances a Component may stand ahead of a
// peer's share on the new revision: the tolerance of its own count, rounded
// up to a whole Instance, at least one, so a Component level with its peer
// always has its step even where one Instance is wider than the tolerance.
func lockstepAllowance(tolerancePercent, count int32) int64 {
	if tolerancePercent < 0 {
		tolerancePercent = 0
	}
	return max(1, ceilDiv(int64(tolerancePercent)*int64(count), 100))
}

// ceilDiv is n / d rounded up, for n >= 0 and d > 0.
func ceilDiv(n, d int64) int64 {
	return (n + d - 1) / d
}

// progressReference is the count each member's roll is measured against by
// the lockstep: the Instance count its spec asks for now where the gate
// observed it (Desired), else the recorded reference (Original). The two
// agree once the ISVC pass has rewritten the reference after a resize; in
// the pass between, the live desired count already decides, so a side
// resized below its start count reads finished at once and a side resized
// above it is measured over the count it rolls toward.
func (s RatioState) progressReference() map[v1beta1.ComponentType]int32 {
	if len(s.Desired) == 0 {
		return s.Original
	}
	out := make(map[v1beta1.ComponentType]int32, len(s.Original))
	for c, orig := range s.Original {
		if desired, ok := s.Desired[c]; ok {
			out[c] = desired
			continue
		}
		out[c] = orig
	}
	return out
}

// lockstepReference is progressReference without the pairing peer the
// pairing gates hold still: that peer moves only once this Component has,
// so it paces this Component's roll no more than a finished peer does.
func (s RatioState) lockstepReference() map[v1beta1.ComponentType]int32 {
	reference := s.progressReference()
	if s.HeldStillPeer == "" {
		return reference
	}
	if _, held := reference[s.HeldStillPeer]; !held {
		return reference
	}
	out := make(map[v1beta1.ComponentType]int32, len(reference))
	for c, n := range reference {
		if c != s.HeldStillPeer {
			out[c] = n
		}
	}
	return out
}

// LockstepHold answers the ratio gate for a step that leaves serving
// capacity unchanged: an in-place patch returns the same pod, so there is
// no capacity change to project against the band, but the Instance still
// moves to the new revision, so the new-revision lockstep applies. It
// reports held=true, with the operator-visible reason, when the patch would
// carry `component`, counting the moves it has under way, past its
// allowance ahead of a still-rolling peer.
// The same preconditions as EvaluateSurge admit the step outright: a group
// without RatioBalanced pacing, a single member, or no resolved tolerance.
func LockstepHold(pacing v1beta1.CoordinationPacing, state RatioState, component v1beta1.ComponentType) (held bool, reason string) {
	if pacing.Type != v1beta1.CoordinationPacingRatioBalanced || len(state.Original) <= 1 || pacing.RatioTolerancePercent == nil {
		return false, ""
	}
	if outpacesPeerProgress(state.lockstepReference(), state.NewPods, state.OpenAttempts, component, *pacing.RatioTolerancePercent) {
		return true, "in-place update would outpace peer new-revision rollout past tolerance"
	}
	return false, ""
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

// SnapshotOriginal records the desired replica counts the ratio reference
// is taken from. Caller passes each Component's desired count; outputs are
// int32 because ResolveGroup / status writers expect that shape.
//
// Components with a count of 0 are recorded as 1 — a zero-replica
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
// excluded. Mirrors escalation.CurrentUnavailableInFlight on the v1beta1
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
		case workloadtypes.UpdateStepSurge, workloadtypes.UpdateStepSurgeDrain,
			workloadtypes.UpdateStepSurgeDrainSettle, workloadtypes.UpdateStepGangSurgeTarget:
			// surge (single-pod or gang replacement) adds capacity, it
			// doesn't remove a pod from rotation
		default:
			n++
		}
	}
	return n
}

// creatingInstances counts a Component's Instances whose row carries a
// Create attempt: the Instances a scale-up is still bringing up, which serve
// nothing yet and have lost nothing.
func creatingInstances(statuses []v1beta1.OMENativeInstanceStatus) int32 {
	var n int32
	for _, s := range statuses {
		if s.Operation != nil && s.Operation.Type == v1beta1.InstanceOperationCreate {
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
		case workloadtypes.UpdateStepSurge, workloadtypes.UpdateStepSurgeDrain, workloadtypes.UpdateStepSurgeDrainSettle:
			n++
		}
	}
	return n
}

// openAttempts counts a replica's Instances whose move toward target has
// started and not landed, each row read as the batch step reads it
// (classifyBatchRow): an attempt stays on the row from the start that
// writes it to the promote; one the batch step reads as ended (Failed, or
// set aside without serving) is not under way, and a row on the target
// has landed.
func openAttempts(rows []v1beta1.OMENativeInstanceStatus, target string) int32 {
	var n int32
	for i := range rows {
		if classifyBatchRow(&rows[i], target) == batchRowOpen {
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

// servingInstanceCount counts a Component's Instances in rotation: the serving
// rows the replica publishes, a replacement gang serving beside its source
// counted once with that source. The budgets and the ratio rule read the
// fleet in Instances, so an open gang surge never reads as capacity.
func servingInstanceCount(summary *v1beta1.InferenceReplicaStatus) int32 {
	return max(summary.ServingReplicas-servingSurgePeakInFlight(summary.InstanceStatuses), 0)
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
	allowed, reason, _ = ctx.checkRatio(inFlightSurge, inFlightUnavail, projDelta, "")
	return allowed, reason
}

// checkRatio is CheckRatio with the band's own reading of an admitted step:
// outsideTolerance is true when the step was admitted although no single
// step of the Component fits the tolerance at the group's shape, so the
// caller can say so on the service. heldStillPeer is the pairing peer the
// pairing gate holds still for this start, which the lockstep does not
// pace by; empty when there is none.
func (ctx GateContext) checkRatio(inFlightSurge, inFlightUnavail, projDelta int32, heldStillPeer v1beta1.ComponentType) (allowed bool, reason string, outsideTolerance bool) {
	if ctx.ShortCircuit {
		return true, ctx.ShortReason, false
	}
	state, decided, allowed, reason := ctx.ratioState(inFlightSurge, inFlightUnavail)
	if decided {
		return allowed, reason, false
	}
	state.HeldStillPeer = heldStillPeer
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
	decision := EvaluateSurge(ctx.Group.Pacing, state, ctx.Component, projDelta)
	if decision.AllowedSurgeDelta != 0 {
		return true, decision.Reason, decision.OutsideTolerance
	}
	return false, decision.Reason, false
}

// checkRatioLockstep is the ratio gate for an in-place start, which keeps
// the pod and so its serving capacity: the serving-capacity band is waived
// (there is no capacity change to project, and projecting a -1 loss would
// over-block the smaller Component of a pair for good) while the
// new-revision lockstep still applies, since the patched Instance moves to
// the new revision like any other. Same prelude and reads as CheckRatio.
// heldStillPeer is the pairing peer the pairing gate holds still, empty
// when there is none: the lockstep does not pace the start by that peer.
func (ctx GateContext) checkRatioLockstep(heldStillPeer v1beta1.ComponentType) (allowed bool, reason string) {
	if ctx.ShortCircuit {
		return true, ctx.ShortReason
	}
	state, decided, allowed, reason := ctx.ratioState(0, 0)
	if decided {
		return allowed, reason
	}
	state.HeldStillPeer = heldStillPeer
	if held, why := LockstepHold(ctx.Group.Pacing, state, ctx.Component); held {
		return false, why
	}
	return true, "in-place update within new-revision lockstep; serving band waived for a same-pod return"
}

// ratioState reads the group's recorded anchor and every member's live
// InferenceReplica status into the RatioState the pacing math evaluates.
// decided is true when the prelude already answers the gate (no
// RatioBalanced pacing, a single member, no anchor yet, or a failed read),
// with allowed and reason set for the caller to return as they are.
func (ctx GateContext) ratioState(inFlightSurge, inFlightUnavail int32) (state RatioState, decided bool, allowed bool, reason string) {
	group := ctx.Group
	isvc := ctx.ISVC
	component := ctx.Component
	if group.Pacing.Type != v1beta1.CoordinationPacingRatioBalanced {
		return RatioState{}, true, true, "not RatioBalanced pacing"
	}
	if len(group.Components) <= 1 {
		return RatioState{}, true, true, "single-component group"
	}

	// Original is read from the recorded ObservedRatio. Live shape,
	// serving, progress, and operations are read from authoritative IR status.
	state = RatioState{
		Original:         make(map[v1beta1.ComponentType]int32, len(group.Components)),
		Current:          make(map[v1beta1.ComponentType]int32, len(group.Components)),
		Desired:          make(map[v1beta1.ComponentType]int32, len(group.Components)),
		NewPods:          make(map[v1beta1.ComponentType]int32, len(group.Components)),
		OpenAttempts:     make(map[v1beta1.ComponentType]int32, len(group.Components)),
		Serving:          make(map[v1beta1.ComponentType]int32, len(group.Components)),
		RecoveryEligible: make(map[v1beta1.ComponentType]bool, len(group.Components)),
		Creating:         make(map[v1beta1.ComponentType]int32, len(group.Components)),
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
		return RatioState{}, true, true, "no ObservedRatio snapshot yet (first reconcile)"
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
		ir, _, err := irprojector.DecodedComponentIRFor(ctx.Ctx, ctx.Reads, isvc, c)
		if err != nil {
			return RatioState{}, true, false, fmt.Sprintf("cannot read %s IR status, failing closed: %v", c, err)
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
		// Serving is what the band check projects against: live traffic
		// capacity in Instances, not rollout progress.
		serving := servingInstanceCount(summary)
		state.Serving[c] = serving
		state.RecoveryEligible[c] = desired > 0 && serving < desired
		state.Creating[c] = creatingInstances(summary.InstanceStatuses)
		state.InFlightSurge[c] = surgeInFlight(summary.InstanceStatuses)
		// Operation-tracked in-flight drains (fresh; see RatioState doc).
		state.InFlightUnavail[c] = unavailableInFlight(summary.InstanceStatuses)
		// Moves under way toward the revision the member's roll moves to;
		// the acting Component's toward the one this pass moves it to.
		target := rollTarget(ir)
		if c == component && ctx.TargetRevision != nil {
			target = ctx.TargetRevision.Name
		}
		state.OpenAttempts[c] = openAttempts(summary.InstanceStatuses, target)
	}
	if !recoveryObservable {
		clear(state.RecoveryEligible)
	}
	// Account for pods this wake-up has already started pulling out of
	// rotation but whose status-aggregate write hasn't landed yet. Only
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
	return state, false, true, ""
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
// snapshot answer and the whole Component drains at once.
//
// Returns allowed=true when:
//   - The Component is not in any coord group,
//   - The component has no observed status yet (first reconcile),
//   - OR (currentUnavailable + inFlightDelta + 1) stays within
//     MaxUnavailableBudget for the Component, scaled against the Instance
//     count the Component's spec asks for (desiredInstanceCount), the
//     same base currentUnavailable is read against.
//
// Counts Instances serving traffic (servingInstanceCount: the serving rows,
// a replacement gang serving beside its source counted once with it), NOT
// pods with merely running containers (ReadyReplicas). The two diverge
// during in-place updates: kubelet still reports ContainersReady while the
// controller has flipped the serving gate to False; using ReadyReplicas
// would mislead the gate into letting every pod get pulled from traffic at
// once. The published count carries an Instance as serving whenever its
// pods were in rotation at the last status write, a crash-looping one
// between two of its crashes included, so the gate also reads the
// Component's pods now: an Instance the counters carry as serving whose
// routed pod restarted or was rebuilt and has not held Ready for the
// proven window since, or with an attempt opened since the write, counts
// as out of rotation too, and the hold names it (servingInstancesDown).
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

	// Read the replica: its spec for the desired count and the window, its
	// counters and rows for the Instances in rotation at the last status
	// write, its rows beside the live pods for the Instances those counters
	// carry as serving that are down now, and its rows to name, on a hold,
	// the Instances that count as out of rotation. Only a genuinely missing
	// IR is "no observation yet" (allow); a read error fails closed so a
	// transient apiserver blip can't disable the unavailability budget and
	// authorize a mass drain.
	ir, _, err := irprojector.DecodedComponentIRFor(ctx.Ctx, ctx.Reads, ctx.ISVC, ctx.Component)
	if err != nil {
		return false, fmt.Sprintf("cannot read %s IR status, failing closed: %v", ctx.Component, err)
	}

	if ir == nil {
		return true, "no observation yet"
	}

	desired := desiredInstanceCount(ir)
	// The Instances out of rotation are the count the spec asks for less
	// the Instances serving traffic, not merely running containers: an
	// in-place drain takes a pod out of rotation before the kubelet sees a
	// restart, and a replacement gang serving beside its source is one
	// Instance with that source. A replica that records no count is read
	// over its rows on both sides, the replacement row among them.
	serving := servingInstanceCount(&ir.Status)
	if !specRecordsInstanceCount(ir) {
		serving = ir.Status.ServingReplicas
	}
	currentUnavailable := max(desired-serving, 0)
	down, notServing, err := ctx.servingInstancesDown(ir)
	if err != nil {
		return false, fmt.Sprintf("cannot list %s pods, failing closed: %v", ctx.Component, err)
	}
	currentUnavailable += int32(len(down))
	inFlightDelta = max(inFlightDelta, 0)
	budget := MaxUnavailableBudget(ctx.Group.Pacing, desired)
	if currentUnavailable+inFlightDelta+1 > budget {
		// The hold names the Instances the count stands for, by ordinal and
		// with why: a scale-up's Instances still being created, an Instance
		// down beside the roll, one up only between its crashes. The numbers
		// alone read as a budget spent by nothing the operator can see.
		return false, UnavailableBudgetHold(budget, currentUnavailable, inFlightDelta,
			unavailableInstancesClause(ir.Status.InstanceStatuses, desired, podsPerInstance(ir)), notServing)
	}
	return true, "within unavailable budget"
}

// UnavailableBudgetHold is the Budget hold's text on the group's
// unavailability budget: the budget, the Instances out of rotation now, the
// starts of this pass and what one more would make, then the clause naming
// the Instances the published rows count as out of rotation
// (unavailableInstancesClause, empty when they count none), then every
// Instance the counters carry as serving while its own pods say it is
// down, each with why (workloadops.ServingInstancesDown).
func UnavailableBudgetHold(budget, current, inFlight int32, rowsClause string, notServing []string) string {
	reason := fmt.Sprintf("unavailable budget %d exhausted (current %d, in-flight %d, would become %d)%s",
		budget, current, inFlight, current+inFlight+1, rowsClause)
	if len(notServing) > 0 {
		reason += "; " + strings.Join(notServing, "; ")
	}
	return reason
}

// servingInstancesDown lists the Instances the replica's counters carry as
// serving whose own pods, read now, say otherwise: a routed pod that
// restarted or was rebuilt and has not held Ready for the proven window
// since, or one that left rotation since the counters were written
// (workloadops.ServingInstancesDown, the reading the Component's own
// budget takes). The window is the Component's minReadySeconds or the
// operator's grace for a pod the kubelet keeps restarting, whichever is
// longer; the pods are the ones the caller handed the gate for the pass,
// else they are listed under the Component's replica prefix through the
// reader the gate reads the replica with. A replica with no rows has nothing
// to read.
func (ctx GateContext) servingInstancesDown(ir *v1beta1.InferenceReplica) ([]int32, []string, error) {
	if len(ir.Status.InstanceStatuses) == 0 {
		return nil, nil, nil
	}
	rows := make([]workloadtypes.InstanceStatus, 0, len(ir.Status.InstanceStatuses))
	for i := range ir.Status.InstanceStatuses {
		rows = append(rows, v1beta1convert.InstanceStatusToWorkload(ir.Status.InstanceStatuses[i]))
	}
	pods, err := ctx.componentPods()
	if err != nil {
		return nil, nil, err
	}
	byInstance := make(map[int32][]*corev1.Pod, len(rows))
	for _, pod := range pods {
		if idx, ok := query.InstanceIdxFromLabels(pod); ok {
			byInstance[idx] = append(byInstance[idx], pod)
		}
	}
	perInstance := podsPerInstance(ir)
	window := workloadops.ProvenWindowSeconds(ir.Spec.MinReadySeconds, ctx.Defaults.ProvingGrace)
	target, born := "", time.Time{}
	if ctx.TargetRevision != nil {
		target, born = ctx.TargetRevision.Name, ctx.TargetRevision.CreationTimestamp.Time
	}
	indices, reasons, _ := workloadops.ServingInstancesDown(rows, byInstance,
		func(*workloadtypes.InstanceStatus) int32 { return perInstance }, target, born, window, time.Now())
	return indices, reasons, nil
}

// componentPods is the Component's own pods the unavailability gate reads:
// the pass's list when the caller handed one, else a list through the reader.
func (ctx GateContext) componentPods() ([]*corev1.Pod, error) {
	if ctx.ComponentPods != nil {
		return ctx.ComponentPods()
	}
	return query.ListOMENativePodsByName(ctx.Ctx, ctx.Reads, ctx.ISVC.Namespace, prefixFor(ctx.ISVC, ctx.Component), v1beta1convert.ComponentTypeToWorkload(ctx.Component), false)
}

// desiredInstanceCount is the Instance count a Component's spec asks for,
// the base a percent budget is scaled against. The published row count is
// not that count while a gang surge is open: the replacement gang is a row
// of its own beside its source until the old gang leaves, so the rows
// overstate the fleet by one per open gang surge. A replica that records no
// count is sized by its rows, and one with neither by a single Instance.
func desiredInstanceCount(ir *v1beta1.InferenceReplica) int32 {
	if specRecordsInstanceCount(ir) {
		return *ir.Spec.Replicas
	}
	if ir.Status.Replicas > 0 {
		return ir.Status.Replicas
	}
	return 1
}

// specRecordsInstanceCount says whether the replica's spec asks for a
// positive Instance count.
func specRecordsInstanceCount(ir *v1beta1.InferenceReplica) bool {
	return ir.Spec.Replicas != nil && *ir.Spec.Replicas > 0
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
//     MaxSurgeBudget for the Component, scaled against the Instance count
//     the Component's spec asks for (desiredInstanceCount).
//
// currentInFlightSurge = count of Instances whose Operation.Step is
// a surge lifecycle step — those are the Instances that
// already have an extra pod alive from a prior wake-up.
func (ctx GateContext) CheckSurge(inFlightDelta int32) (allowed bool, reason string) {
	if ctx.ShortCircuit {
		return true, ctx.ShortReason
	}

	// Read the replica: its spec for the desired count, its decoded rows
	// for the surges in flight. Only a genuinely missing IR is "no
	// observation yet" (allow); a read error fails closed so a transient
	// apiserver blip can't erase the in-flight surge count and blow the
	// surge budget.
	ir, _, err := irprojector.DecodedComponentIRFor(ctx.Ctx, ctx.Reads, ctx.ISVC, ctx.Component)
	if err != nil {
		return false, fmt.Sprintf("cannot read %s IR status, failing closed: %v", ctx.Component, err)
	}

	if ir == nil {
		return true, "no observation yet"
	}

	desired := desiredInstanceCount(ir)
	currentInFlight := surgeInFlight(ir.Status.InstanceStatuses)
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
