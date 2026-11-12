package coordination

import (
	"cmp"
	"slices"
	"time"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// ComponentObservation captures the inputs the state machine reads for
// one Component in a group: replica counts, per-revision pod counts,
// in-flight failure / pause hints. Populated by the caller from
// the status aggregate plus live pod reads.
type ComponentObservation struct {
	// Component identifies this observation.
	Component v1beta1.ComponentType

	// DesiredReplicas is the replica's status.replicas: the Instance rows it
	// manages, in any phase. Zero until its status is first written.
	DesiredReplicas int32

	// SpecReplicas is the Instance count the replica's spec asks for now, the
	// operator's latest intent (minReplicas, or an autoscaler's count), read
	// as at least one since a count of zero cannot anchor a ratio. Zero only
	// while the replica does not exist yet.
	SpecReplicas int32

	// TotalPods is the live pod count, any revision.
	TotalPods int32

	// ReadyPods is the live pod count reporting Ready=True.
	ReadyPods int32

	// ServingPods is the Instance count actually in load-balancer rotation
	// (serving=True), a replacement gang serving beside its source counted
	// once with that source. Diverges from ReadyPods during in-place
	// drains where the serving gate is flipped before containers
	// restart. RatioBalanced math projects against this — operators
	// care about live traffic capacity, not rollout progress.
	ServingPods int32

	// NewRevisionPods counts the Instances whose row runs the target
	// ControllerRevision (UpdateRevision in LifecycleStatus terms): the
	// replica's updated count, not live pods, so a row keeps the target
	// while its pod is unready or gone. When zero, the Component is not
	// yet surging.
	NewRevisionPods int32

	// NewRevisionReadyPods is the subset of NewRevisionPods whose pods
	// report Ready=True.
	NewRevisionReadyPods int32

	// TargetRevisionHash is the ControllerRevision hash the Component
	// is converging toward. Empty when no rollout is in flight (the
	// Component is already at its desired revision).
	TargetRevisionHash string

	// CurrentRevisionHash is the ControllerRevision hash the
	// Component is currently serving.
	CurrentRevisionHash string

	// RolloutInFlight is true when DesiredRevisionHash !=
	// CurrentRevisionHash. The state machine routes off this flag
	// rather than recomputing it from the two hashes so callers can
	// override (e.g., for tests).
	RolloutInFlight bool

	// Partition is the component's desired static rollingUpdate.partition
	// (instances with index < Partition are held on the prior revision).
	// 0 = full rollout. Resolved only for non-canary groups; canary-owned
	// components leave it 0 (they use the canary reconciler's own path).
	Partition int32

	// AtDesiredShape is true when the component rests at its staged shape
	// (status.ReachedDesiredShape): every Instance Ready, the ones still on
	// the prior revision held by the partition, at most Partition of them
	// and every one of them when fewer than Partition remain. The state
	// machine rests (Staged) and Sequential hands off on this.
	AtDesiredShape bool

	// ReadyAt is the instant the Component's replica last reported its roll
	// done: the Ready condition's last transition to True, which the replica
	// writes once every Instance is Ready on the target or, under a
	// partition, at its staged shape. Zero while the condition is not True.
	// The Sequential wait after this Component is measured from it while the
	// group has recorded no finish.
	ReadyAt time.Time

	// NotReady is true when the replica's Ready condition is False or
	// absent: every pod of the Component is out of rotation, or its replica
	// was recreated and has published no condition yet. Unknown, the
	// condition while a roll is recorded, is neither Ready nor NotReady. A
	// Sequential chain holds the next member while the finished one reads
	// NotReady.
	NotReady bool

	// Failed is true when the Component's OMENative reconciler
	// flagged a terminal failure (Ready timeout, persistent op error).
	Failed bool

	// FailureMessage is the operator-facing explanation when Failed.
	FailureMessage string

	// UpdateStrategy is the strategy the Component's starts run on:
	// SurgeThenDrain (the default when unset), RecreatePod for every
	// drain-first shape (an in-place strategy on a gang rebuilds it), or an
	// in-place strategy on single-pod Instances. It names the move a hold
	// refuses; no phase is decided on it.
	UpdateStrategy workloadtypes.UpdateStrategyType
}

// GroupObservation collects per-Component observations plus group-wide
// hints the state machine reads.
type GroupObservation struct {
	// Group is the resolved coordination group.
	Group ResolvedGroup

	// Components maps Component → observation. Includes every
	// Component listed in Group.Components.
	Components map[v1beta1.ComponentType]ComponentObservation

	// OriginalReplicas is the snapshot the RatioBalanced pacing math
	// reads. Snapshotted at rollout start; nil when no rollout has
	// started yet.
	OriginalReplicas map[v1beta1.ComponentType]int32

	// PausedGlobal is true when the operator paused the whole
	// rollout via the ome.io/rollout-paused annotation on the ISVC
	// (either accepted value — pause and freeze hold coordination
	// identically). The state machine honors this by leaving every
	// group in Paused until the annotation is removed.
	PausedGlobal bool

	// Now is the reference time the state machine compares against
	// when evaluating time-based gates (Sequential soak). Caller
	// passes time.Now() at the top of the reconcile pass so all gates
	// see the same instant.
	Now time.Time

	// PreviousPhaseEnteredAt is the timestamp the group last entered
	// its current Phase, as recorded in the previously-written status.
	// The phase-dwell metric reads it; no transition is decided on it,
	// since it moves on every base-phase change. Zero when no prior
	// status is available (first reconcile).
	PreviousPhaseEnteredAt time.Time

	// MemberFinished is the finish record the previously-written status
	// carries for a Sequential group: the member whose roll most recently
	// read done and the instant it first read so. Zero when the status
	// holds none.
	MemberFinished MemberFinish
}

// MemberFinish is the instant a Sequential member's roll first read done,
// kept in the group's status so the wait before the next member is measured
// from one instant on every pass, on the group and on the replica side. A
// readiness loss or a recreated replica of the member does not move it.
type MemberFinish struct {
	// Component names the member.
	Component v1beta1.ComponentType

	// At is the instant the member first read done.
	At time.Time
}

// IsZero reports whether no finish is recorded.
func (f MemberFinish) IsZero() bool {
	return f.Component == "" || f.At.IsZero()
}

// GroupTransition is the state machine's decision for one group this
// reconcile. The caller applies the decision (writes status, surges
// pods, drains, etc.) without re-reading the state.
type GroupTransition struct {
	// Phase is the next base phase.
	Phase v1beta1.CoordinationPhase

	// CompositePhase is the operator-facing composite phase string
	// (e.g., `decoder.Surging` for Sequential).
	CompositePhase string

	// CurrentComponent names the Component the group is actively
	// reconciling (Sequential only).
	CurrentComponent v1beta1.ComponentType

	// PreviousComponent names the most recently completed Component
	// (Sequential only).
	PreviousComponent v1beta1.ComponentType

	// Message is the operator-facing explanation of the phase.
	Message string

	// RatioSkewRejected is true when at least one Component's surge
	// budget was reduced to zero by RatioBalanced pacing this reconcile.
	// Surfaces the EvaluateSurge SkewRejected signal so callers can
	// emit the RatioSkewRejected event + increment the ratio_skew_total
	// counter. Computed in every phase in which a member's next start may
	// still be asked of the ratio gate: Surging, Waiting, and, while a
	// rolling member keeps a pod on an old revision, Shifting, ScalingDown
	// and Failed. Always false for PerComponent pacing, at rest (Idle,
	// Staged) and under a pause.
	RatioSkewRejected bool

	// RatioHeldStarts names the Components whose next start RatioBalanced
	// pacing holds this reconcile, in the group's order, each with the
	// strategy that start runs on. Empty exactly when RatioSkewRejected is
	// false.
	RatioHeldStarts []HeldStart

	// MemberFinished is the finish record the group's status carries after
	// this pass (Sequential with a wait between members only). Zero when
	// there is none to carry.
	MemberFinished MemberFinish

	// MemberNotReady names the finished Sequential member whose readiness
	// holds the next member this pass. Empty otherwise.
	MemberNotReady v1beta1.ComponentType
}

// HeldStart is one Component whose next start RatioBalanced pacing holds,
// with the strategy the start runs on: the move an operator reads in the
// event is a surge start, a recreate or an in-place restart by it.
type HeldStart struct {
	Component v1beta1.ComponentType
	Strategy  workloadtypes.UpdateStrategyType
}

// ComputeTransition walks the group's state machine for this reconcile
// and returns the decision. Pure function — no I/O, no status writes.
// Status writers translate the decision; tests assert against this
// shape directly.
func ComputeTransition(obs GroupObservation) GroupTransition {
	if obs.PausedGlobal {
		return GroupTransition{
			Phase:          v1beta1.CoordinationPhasePaused,
			CompositePhase: string(v1beta1.CoordinationPhasePaused),
			Message:        "rollout paused via ome.io/rollout-paused annotation",
		}
	}
	switch obs.Group.Policy {
	case v1beta1.CoordinationPolicyBlueGreen:
		return blueGreenTransition(obs)
	case v1beta1.CoordinationPolicyRollingUpdate:
		return rollingUpdateTransition(obs)
	case v1beta1.CoordinationPolicySequential:
		return sequentialTransition(obs)
	case v1beta1.CoordinationPolicyIndependent:
		fallthrough
	default:
		return independentTransition(obs)
	}
}

// blueGreenTransition implements the gang-style state machine: surge all
// Components together, wait for all Ready, shift traffic together,
// drain, scale down. Failure in any Component fails the whole group.
func blueGreenTransition(obs GroupObservation) GroupTransition {
	if anyMemberFailed(obs) {
		return phaseWithHeldStarts(v1beta1.CoordinationPhaseFailed, "one or more Components in the BlueGreen group failed", ratioHeldStartsWhileStartsRemain(obs))
	}
	if !anyRollInFlight(obs) {
		return phaseOnly(v1beta1.CoordinationPhaseIdle, "no rollout in flight")
	}
	switch {
	case !everyMemberHasTargetPods(obs):
		return phaseWithHeldStarts(v1beta1.CoordinationPhaseSurging, "creating new-revision pods", ratioHeldStartsWhileStartsRemain(obs))
	case !everyTargetPodReady(obs):
		return phaseWithHeldStarts(v1beta1.CoordinationPhaseWaiting, "waiting for new-revision pods to be Ready", ratioHeldStartsWhileStartsRemain(obs))
	}
	if restsAtPartition(obs) {
		return phaseOnly(v1beta1.CoordinationPhaseStaged, "converged to partitioned target; holding old-revision pods by design")
	}
	// The phases after Ready collapse into one Shifting decision: the status
	// writer, which sees the traffic delta, advances to Draining and
	// ScalingDown from whether the producer has written and whether old
	// pods still exist.
	if anyMemberKeepsOldPods(obs) {
		return phaseWithHeldStarts(v1beta1.CoordinationPhaseShifting, "shifting traffic; old-revision pods still serving", ratioHeldStartsWhileStartsRemain(obs))
	}
	return phaseOnly(v1beta1.CoordinationPhaseScalingDown, "scaling down old-revision pods")
}

// everyMemberHasTargetPods reports whether every member, rolling or at rest,
// has a pod on the target revision: the gang's surge is complete only then.
func everyMemberHasTargetPods(obs GroupObservation) bool {
	for _, c := range obs.Components {
		if c.NewRevisionPods == 0 {
			return false
		}
	}
	return true
}

// everyTargetPodReady reports whether every member has pods on the target
// revision and all of them are Ready.
func everyTargetPodReady(obs GroupObservation) bool {
	for _, c := range obs.Components {
		if c.NewRevisionReadyPods < c.NewRevisionPods || c.NewRevisionPods == 0 {
			return false
		}
	}
	return true
}

// anyMemberKeepsOldPods reports whether a member, rolling or at rest, still
// has a pod on a revision other than the target.
func anyMemberKeepsOldPods(obs GroupObservation) bool {
	for _, c := range obs.Components {
		if c.TotalPods > c.NewRevisionPods {
			return true
		}
	}
	return false
}

// rollingUpdateTransition implements RollingUpdate: per-Component
// state machines walk in parallel; the group phase is the union (any
// Component still surging means the group is Surging; otherwise
// Waiting → Shifting → ScalingDown → Idle).
func rollingUpdateTransition(obs GroupObservation) GroupTransition {
	if anyMemberFailed(obs) {
		return phaseWithHeldStarts(v1beta1.CoordinationPhaseFailed, "one or more Components in the RollingUpdate group failed", ratioHeldStartsWhileStartsRemain(obs))
	}
	if !anyRollInFlight(obs) {
		return phaseOnly(v1beta1.CoordinationPhaseIdle, "no rollout in flight")
	}
	rolls := readMemberRolls(obs)
	if !rolls.surging && !rolls.waiting && restsAtPartition(obs) {
		return phaseOnly(v1beta1.CoordinationPhaseStaged, "converged to partitioned target; holding old-revision pods by design")
	}
	return rollingUpdatePhase(obs, rolls)
}

// anyMemberFailed reports whether a member's reconciler flagged a terminal
// failure.
func anyMemberFailed(obs GroupObservation) bool {
	for _, c := range obs.Components {
		if c.Failed {
			return true
		}
	}
	return false
}

// anyRollInFlight reports whether a member has a roll in flight.
func anyRollInFlight(obs GroupObservation) bool {
	for _, c := range obs.Components {
		if c.RolloutInFlight {
			return true
		}
	}
	return false
}

// memberRolls is what the members with a roll in flight are doing; a member
// at rest is not read.
type memberRolls struct {
	// surging is true when a rolling member has no pod on the target revision.
	surging bool
	// waiting is true when a rolling member has a target-revision pod that
	// is not Ready.
	waiting bool
	// scalingDown is true when a rolling member keeps old-revision pods
	// beside Ready target-revision pods.
	scalingDown bool
}

// readMemberRolls reads each rolling member for the first of surging,
// waiting and scaling down that describes it.
func readMemberRolls(obs GroupObservation) memberRolls {
	var rolls memberRolls
	for _, c := range obs.Components {
		switch {
		case !c.RolloutInFlight:
		case c.NewRevisionPods == 0:
			rolls.surging = true
		case c.NewRevisionReadyPods < c.NewRevisionPods:
			rolls.waiting = true
		case c.TotalPods > c.NewRevisionPods:
			rolls.scalingDown = true
		}
	}
	return rolls
}

// restsAtPartition reports whether every member with a roll in flight rests
// at the shape its partition asks for and at least one of them holds
// old-revision pods by that partition. A member with no roll in flight rests
// where it is, whatever partition its spec carries, so it neither holds the
// group from Staged nor counts as holding Instances.
func restsAtPartition(obs GroupObservation) bool {
	anyPartition := false
	for _, c := range obs.Components {
		if !c.RolloutInFlight {
			continue
		}
		if !c.AtDesiredShape {
			return false
		}
		if c.Partition > 0 {
			anyPartition = true
		}
	}
	return anyPartition
}

// rollingUpdatePhase is the union of the rolling members' phases: Surging
// before Waiting before ScalingDown, else Shifting. The starts the ratio
// gate holds are named in every phase in which a start may still be asked.
func rollingUpdatePhase(obs GroupObservation, rolls memberRolls) GroupTransition {
	switch {
	case rolls.surging:
		_, held := computeSurgeBudgetWithRatio(obs)
		return phaseWithHeldStarts(v1beta1.CoordinationPhaseSurging, "at least one Component is surging", held)
	case rolls.waiting:
		_, held := computeSurgeBudgetWithRatio(obs)
		return phaseWithHeldStarts(v1beta1.CoordinationPhaseWaiting, "waiting for new-revision pods to be Ready", held)
	case rolls.scalingDown:
		return phaseWithHeldStarts(v1beta1.CoordinationPhaseScalingDown, "scaling down old-revision pods", ratioHeldStartsWhileStartsRemain(obs))
	default:
		return phaseOnly(v1beta1.CoordinationPhaseShifting, "writing per-Component traffic weights")
	}
}

// phaseWithHeldStarts is the decision for a phase in which a start may still
// be asked of the ratio gate: the skew signal is raised exactly when the
// gate holds one.
func phaseWithHeldStarts(phase v1beta1.CoordinationPhase, message string, held []HeldStart) GroupTransition {
	return GroupTransition{
		Phase:             phase,
		CompositePhase:    string(phase),
		Message:           message,
		RatioSkewRejected: len(held) > 0,
		RatioHeldStarts:   held,
	}
}

// phaseOnly is the decision for a phase in which no start can be asked.
func phaseOnly(phase v1beta1.CoordinationPhase, message string) GroupTransition {
	return GroupTransition{Phase: phase, CompositePhase: string(phase), Message: message}
}

// independentTransition collapses every rolling Component into the
// group's phase but skips the gang-readiness check. Used both for
// explicit Independent groups and as the implicit default when no
// group is declared (callers that drive an InferenceService without a
// rolloutCoordination block hit this branch).
func independentTransition(obs GroupObservation) GroupTransition {
	anyRolling := false
	failed := false
	for _, c := range obs.Components {
		if c.Failed {
			failed = true
		}
		if c.RolloutInFlight {
			anyRolling = true
		}
	}
	if failed {
		return GroupTransition{
			Phase:          v1beta1.CoordinationPhaseFailed,
			CompositePhase: string(v1beta1.CoordinationPhaseFailed),
			Message:        "one or more Components in the Independent group failed",
		}
	}
	if !anyRolling {
		return GroupTransition{
			Phase:          v1beta1.CoordinationPhaseIdle,
			CompositePhase: string(v1beta1.CoordinationPhaseIdle),
			Message:        "no rollout in flight",
		}
	}
	return GroupTransition{
		Phase:          v1beta1.CoordinationPhaseShifting,
		CompositePhase: string(v1beta1.CoordinationPhaseShifting),
		Message:        "independent per-Component rollouts in flight",
	}
}

// sequentialTransition implements Sequential: walk the group's
// Order one Component at a time. The next Component cannot enter
// Surging until the previous Component is back at Idle on its new
// revision.
func sequentialTransition(obs GroupObservation) GroupTransition {
	if failed, ok := sequentialFailedMember(obs); ok {
		return sequentialBlocked(obs, failed)
	}
	if c, ok := activeSequentialComponent(obs.Group.Order, obs.Components); ok {
		return sequentialActive(obs, c)
	}
	return sequentialAtRest(obs)
}

// sequentialFailedMember is the first member of the order whose reconciler
// flagged a terminal failure; a member outside the order never counts.
func sequentialFailedMember(obs GroupObservation) (v1beta1.ComponentType, bool) {
	for _, c := range obs.Group.Order {
		if comp, ok := obs.Components[c]; ok && comp.Failed {
			return c, true
		}
	}
	return "", false
}

// sequentialBlocked is the group's decision while a member is Failed. The
// phase is a rollup of the members' state, with no group-level record, so
// a spec edit that reconciles the member back to health releases the group
// on the next pass.
func sequentialBlocked(obs GroupObservation, failed v1beta1.ComponentType) GroupTransition {
	return GroupTransition{
		Phase:             v1beta1.CoordinationPhaseFailed,
		CompositePhase:    v1beta1.CompositePhaseSequentialFailed,
		CurrentComponent:  failed,
		PreviousComponent: previousCompletedComponent(obs, failed),
		Message:           "Sequential group blocked by Failed Component: " + string(failed),
		MemberFinished:    sequentialMemberFinish(obs, ""),
	}
}

// sequentialActive is the group's decision while member c rolls: the hold
// for the wait after the member before it while that wait runs, else c's
// own per-Component phase as the group's phase.
func sequentialActive(obs GroupObservation, c v1beta1.ComponentType) GroupTransition {
	prev := previousCompletedComponent(obs, c)
	finished := sequentialMemberFinish(obs, prev)
	if held, ok := sequentialWaitHold(obs, c, prev, finished); ok {
		return held
	}
	sub := blueGreenTransition(GroupObservation{
		Group: ResolvedGroup{
			Name:       obs.Group.Name,
			Index:      obs.Group.Index,
			Components: []v1beta1.ComponentType{c},
			Policy:     v1beta1.CoordinationPolicyBlueGreen,
			Pacing:     obs.Group.Pacing,
		},
		Components: map[v1beta1.ComponentType]ComponentObservation{c: obs.Components[c]},
	})
	return GroupTransition{
		Phase:             sub.Phase,
		CompositePhase:    string(c) + "." + string(sub.Phase),
		CurrentComponent:  c,
		PreviousComponent: prev,
		Message:           sub.Message,
		MemberFinished:    finished,
	}
}

// sequentialWaitHold holds c while the operator's wait since prev finished
// still runs. The wait is measured from the recorded finish, the instant
// prev first read done, so prev losing readiness or being recreated neither
// cuts it short nor starts it over. While prev is not Ready, c is held
// whatever the wait says, and the wait keeps running, so prev's recovery
// releases c at once when the wait has elapsed by then.
func sequentialWaitHold(obs GroupObservation, c, prev v1beta1.ComponentType, finished MemberFinish) (GroupTransition, bool) {
	if obs.Group.Soak <= 0 || prev == "" || finished.Component != prev {
		return GroupTransition{}, false
	}
	elapsed := obs.Now.Sub(finished.At)
	held := GroupTransition{
		Phase:             v1beta1.CoordinationPhaseIdle,
		CompositePhase:    v1beta1.CompositePhaseSequentialAwaiting,
		CurrentComponent:  c,
		PreviousComponent: prev,
		MemberFinished:    finished,
	}
	if obs.Components[prev].NotReady {
		held.Message = "Sequential soak: " + string(prev) + " not Ready; "
		if elapsed < obs.Group.Soak {
			held.Message += (obs.Group.Soak - elapsed).String() + " remaining before " + string(c) + " starts, once " + string(prev) + " is Ready"
		} else {
			held.Message += string(c) + " starts once " + string(prev) + " is Ready"
		}
		held.MemberNotReady = prev
		return held, true
	}
	if elapsed < obs.Group.Soak {
		held.Message = "Sequential soak: " + (obs.Group.Soak - elapsed).String() + " remaining before " + string(c) + " starts"
		return held, true
	}
	return GroupTransition{}, false
}

// sequentialAtRest is the group's decision with no member rolling: complete
// when every member of the order is done, waiting for the first change when
// none is, else between the last done member and the next one.
func sequentialAtRest(obs GroupObservation) GroupTransition {
	completed := completedSequentialComponents(obs)
	switch {
	case completed >= len(obs.Group.Order):
		return GroupTransition{
			Phase:          v1beta1.CoordinationPhaseIdle,
			CompositePhase: string(v1beta1.CoordinationPhaseIdle),
			Message:        "Sequential group complete",
			MemberFinished: sequentialMemberFinish(obs, ""),
		}
	case completed == 0:
		return GroupTransition{
			Phase:          v1beta1.CoordinationPhaseIdle,
			CompositePhase: string(v1beta1.CoordinationPhaseIdle),
			Message:        "Sequential group awaiting first Component spec bump",
			MemberFinished: sequentialMemberFinish(obs, ""),
		}
	}
	prev, next := obs.Group.Order[completed-1], obs.Group.Order[completed]
	return GroupTransition{
		Phase:             v1beta1.CoordinationPhaseIdle,
		CompositePhase:    v1beta1.CompositePhaseSequentialAwaiting,
		CurrentComponent:  next,
		PreviousComponent: prev,
		Message:           "Sequential group in soak window between Components",
		MemberFinished:    sequentialMemberFinish(obs, prev),
	}
}

// activeSequentialComponent returns the first Component in `order` that
// is still genuinely unfinished for handoff. A Component is "done for
// handoff" — and thus skipped — when its rollout is not in flight OR it
// has converged to its desired staged shape (AtDesiredShape). The latter
// releases a partitioned Component that keeps RolloutInFlight=true forever
// by design (old pods held by the partition): once it reaches its staged
// shape it must not block the next Component. The state machine
// routes Sequential's "which Component is currently rolling" decision off
// this; the dispatcher's CheckSequentialGate consumes the same helper
// so the two surfaces never disagree on what's active. Returns ok=false
// when no Component in `order` is still unfinished (Sequential is idle).
func activeSequentialComponent(order []v1beta1.ComponentType, components map[v1beta1.ComponentType]ComponentObservation) (v1beta1.ComponentType, bool) {
	for _, c := range order {
		comp, ok := components[c]
		if !ok || !comp.RolloutInFlight || comp.AtDesiredShape {
			continue
		}
		return c, true
	}
	return "", false
}

// completedSequentialComponents counts the number of leading Components
// in the Order that are done for handoff. A Component counts as completed
// when its rollout is not in flight OR it has converged to its desired
// staged shape (!RolloutInFlight || AtDesiredShape). The AtDesiredShape
// clause advances the count past a partitioned leading Component that
// holds old pods by design and never clears RolloutInFlight. The
// count drives Sequential's "which Component is next" decision.
func completedSequentialComponents(obs GroupObservation) int {
	count := 0
	for _, c := range obs.Group.Order {
		comp, ok := obs.Components[c]
		if !ok {
			return count
		}
		if comp.RolloutInFlight && !comp.AtDesiredShape {
			return count
		}
		count++
	}
	return count
}

// previousCompletedComponent returns the Component immediately
// preceding c in the Order whose rollout has completed (or empty when
// c is the first Order entry).
func previousCompletedComponent(obs GroupObservation, c v1beta1.ComponentType) v1beta1.ComponentType {
	for i, oc := range obs.Group.Order {
		if oc != c {
			continue
		}
		if i == 0 {
			return ""
		}
		return obs.Group.Order[i-1]
	}
	return ""
}

// sequentialMemberFinish is the finish record the group's status carries
// after this pass, for a Sequential group with a wait between members: the
// record it already holds while that member still reads done, a fresh one
// when the member before the active one (prev) first reads done, and none
// when the recorded member rolls again or has left the group. A fresh
// record carries the member's Ready transition when its replica has
// recorded the roll done, else now, so the pass between a member's last
// Instance Ready and its promote counts toward the wait. A group with no
// wait carries no record.
func sequentialMemberFinish(obs GroupObservation, prev v1beta1.ComponentType) MemberFinish {
	if obs.Group.Soak <= 0 {
		return MemberFinish{}
	}
	finished := obs.MemberFinished
	if !finished.IsZero() {
		comp, ok := obs.Components[finished.Component]
		if !ok || (comp.RolloutInFlight && !comp.AtDesiredShape) {
			finished = MemberFinish{}
		}
	}
	if prev == "" || finished.Component == prev {
		return finished
	}
	at := obs.Components[prev].ReadyAt
	if at.IsZero() {
		at = obs.Now
	}
	return MemberFinish{Component: prev, At: at}
}

// computeSurgeBudget returns the per-Component MaxSurge budget for the
// group's pacing, keyed by Component, each scaled against the Instance
// count the member's spec asks for. Caller plumbs each value into
// the per-Component surge logic so the operator's MaxSurge bound is
// respected end-to-end. PerComponent-pacing only — for RatioBalanced,
// use computeSurgeBudgetWithRatio.
func computeSurgeBudget(obs GroupObservation) map[v1beta1.ComponentType]int32 {
	pacing := obs.Group.Pacing
	out := make(map[v1beta1.ComponentType]int32, len(obs.Components))
	for c, comp := range obs.Components {
		out[c] = MaxSurgeBudget(pacing, comp.instanceCount())
	}
	return out
}

// instanceCount is the Instance count a percent budget is scaled against:
// the count the spec asks for, else the rows the replica publishes, else
// one. The rows overstate the fleet by one per open gang surge, since the
// replacement gang is a row of its own beside its source until the old gang
// leaves, so they size the budget only while the spec is not observed.
func (c ComponentObservation) instanceCount() int32 {
	switch {
	case c.SpecReplicas > 0:
		return c.SpecReplicas
	case c.DesiredReplicas > 0:
		return c.DesiredReplicas
	}
	return 1
}

// startsRemain reports whether the ratio gate may still be asked about a
// start of this member: its roll is in flight and an Instance beyond its
// partition is not yet on the target. A member that keeps its template, one
// whose movable Instances are all on the target and one resting at its
// partition have no next start, so the skew signal never names them.
func (c ComponentObservation) startsRemain() bool {
	return c.RolloutInFlight && c.NewRevisionPods < c.instanceCount()-c.Partition
}

// ratioHeldStartsWhileStartsRemain is the group's skew signal for a phase
// in which the roll may still have starts to ask of the ratio gate. The
// evaluation names only a member with a start left (startsRemain), so a
// group with nothing left to start holds nothing.
func ratioHeldStartsWhileStartsRemain(obs GroupObservation) []HeldStart {
	_, held := computeSurgeBudgetWithRatio(obs)
	return held
}

// computeSurgeBudgetWithRatio returns SurgeBudget and the Components whose
// next start RatioBalanced pacing holds, in the group's order. For
// PerComponent pacing this is identical to computeSurgeBudget with nothing
// held. For RatioBalanced pacing, EvaluateSurge gates the MaxSurge budget of
// every member that has a start left to ask: when the ratio band would be
// exceeded by even one surge, the budget is zeroed and the Component is held
// so the caller can fire the RatioSkewRejected metrics/events. A member with
// no start left (one that keeps its template, one whose movable Instances
// are all on the target, one resting at its partition) is never asked, so
// a change of one member alone names no hold on the members that stand.
//
// Baseline: populates state.Serving from obs.Components.ServingPods so
// EvaluateSurge projects against live serving capacity, NOT against the
// zero-initialized NewPods that exist at rollout start. Without Serving
// populated EvaluateSurge falls back to NewPods=0 and pairwise band
// checks see a zero-serving peer (mass-drain failure mode), rejecting
// every surge attempt the moment a Component first touches RatioBalanced.
// That spurious rejection produces an unbroken stream of RatioSkewRejected
// events with no progress; the dispatcher's CheckRatioGate may still
// allow drain because it reads ServingReplicas from status, but this
// state-machine-side gating fires the noise + zeros SurgeBudget map.
func computeSurgeBudgetWithRatio(obs GroupObservation) (map[v1beta1.ComponentType]int32, []HeldStart) {
	budget := computeSurgeBudget(obs)
	if obs.Group.Pacing.Type != v1beta1.CoordinationPacingRatioBalanced {
		return budget, nil
	}
	if len(obs.OriginalReplicas) == 0 {
		// No anchor yet — let MaxSurge govern alone, no skew possible
		// until the next reconcile when SnapshotOriginal has run.
		return budget, nil
	}
	state := RatioState{
		Original: obs.OriginalReplicas,
		Current:  make(map[v1beta1.ComponentType]int32, len(obs.Components)),
		NewPods:  make(map[v1beta1.ComponentType]int32, len(obs.Components)),
		Serving:  make(map[v1beta1.ComponentType]int32, len(obs.Components)),
	}
	for c, comp := range obs.Components {
		state.Current[c] = comp.TotalPods
		state.NewPods[c] = comp.NewRevisionPods
		state.Serving[c] = comp.ServingPods
	}
	var held []HeldStart
	for c, proposed := range budget {
		if proposed <= 0 || !obs.Components[c].startsRemain() {
			continue
		}
		d := EvaluateSurge(obs.Group.Pacing, state, c, proposed)
		budget[c] = d.AllowedSurgeDelta
		if d.SkewRejected {
			held = append(held, HeldStart{Component: c, Strategy: obs.Components[c].UpdateStrategy})
		}
	}
	slices.SortFunc(held, func(a, b HeldStart) int {
		if r := cmp.Compare(memberRank(obs.Group, a.Component), memberRank(obs.Group, b.Component)); r != 0 {
			return r
		}
		return cmp.Compare(a.Component, b.Component)
	})
	return budget, held
}

// memberRank orders members as the group declares them; one the group does
// not list sorts after them.
func memberRank(g ResolvedGroup, c v1beta1.ComponentType) int {
	if i := slices.Index(g.Components, c); i >= 0 {
		return i
	}
	return len(g.Components)
}
