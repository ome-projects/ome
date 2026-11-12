package coordination

import (
	"context"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// EvaluateUpdateGate runs the full coordination gate stack for a single
// per-Instance Update decision. It is the decision site the IR-managed
// OMENative path (inferencereplica.Reconciler) wires into
// workload.ReconcileInput.UpdateGate. Leaving UpdateGate nil silently
// disables coordination: CheckSequential / CheckRatio / CheckSurge /
// CheckUnavailability are all skipped, both Components of a Sequential
// group recreate concurrently, and a group-wide MaxSurge never caps a
// Component whose per-Component budget is larger. Keeping the gate body
// here — in the coordination package the IR path depends on — means
// there is exactly one tested implementation.
//
// Gate order: pairing -> batch step | ratio -> surge | unavailability ->
// sequential. First denial wins; later gates short-circuit. The caller
// reads the allowed bit and may also surface gate + denyReason as a
// RolloutHold; gate names Pairing, Ratio (the batch step reports under
// it), Held (the batch step's wait on a named first side whose retry
// record is Held), Budget (surge or unavailability), or Sequential
// accordingly. A member of a prefill/decode canary unit stops after the
// batch step: the canary executor stages its capacity and its own budget
// paces its starts. A member of a recorded group the spec no longer
// declares stops after the pairing gate: its own budget paces its starts.
//
// The batch step takes the ratio gate's place for the members of a group
// whose engine and decoder both step (readBatchStep): a drain-first member
// counts its drains in batches of its maxUnavailable, a surging member its
// surges in batches of its maxSurge, and none opens batch b+1 until every
// one's batch b serves, which keeps the ratio at every step boundary without
// the serving band. The budget gates still cap each wake-up. Under the
// operator's keepRatio setting a pairing member that patches single pods in
// place steps too, and the two sides' batches are planned to the group's
// tolerance; under firstSide the named side moves first inside each batch,
// and while that side's retry record is Held the other side's wait on it
// reads Held, naming the side and its record, so the roll is held as a
// whole until a corrected revision or a release of the record.
//
// Ratio projection is strategy-aware (see the projDelta computation
// below): SurgeThenDrain projects +1 (a Ready pod is added before the old
// drains), drain-first strategies project -1. A hardcoded -1 for surge
// rolls mis-models the surge as a capacity loss and deadlocks symmetric
// RatioBalanced groups.
//
// strategy is the mechanism the start runs on, as the workload reports it
// (ReconcileInput.UpdateGate): an in-place strategy that resolves to a
// recreate on the Instance (a gang, or a single-pod diff beyond container
// images) arrives as RecreatePod, so an in-place value here is a start
// that patches the same pod. In-place starts (InPlaceIfPossible / InPlaceOnly)
// consult only the new-revision lockstep half of the ratio gate
// (checkRatioLockstep): the patched Instance moves to the new revision like
// any other, so a Component already rolled further than its peer by more
// than the tolerance waits. The serving-capacity band is waived for them:
// the drain is paired 1:1 with a same-pod return (mark-not-ready -> patch
// -> mark-ready), so the net capacity change is ~0; projecting a -1 loss
// would misrepresent that transient as permanent and indefinitely over-block
// the smaller Component of a ratio pair (e.g. the decoder in a 4:2
// engine:decoder pair with 25% tolerance deadlocks because 4:1 = 4.0 sits
// above the [1.5, 2.5] band). CheckUnavailability (called below for
// non-surge strategies, which includes both in-place variants) already
// governs concurrent disruption via the MaxUnavailable budget, so
// RatioBalanced safety is preserved without the structural deadlock. The
// RatioGateBypassed event + metric fire so operators see why the band
// did not run.
//
// Reads authoritative InferenceReplica status via the provided context and
// reader. fromRevision is the ControllerRevision the Instance being started
// runs now (its row's RunningRevision), the cohort the pairing gate charges
// the start to; empty when the row records none. targetRevision is the
// ControllerRevision the Component's Instances move to in this pass (the
// rollback revision while a rollback is pinned, else the spec target); the
// pairing gate orients its cohort simulation by that revision's protocol.
// recorder may be nil (the event is best-effort
// observability). defaults carries the operator-configured group fill-ins
// (e.g. the ratio tolerance default) so the gates see the same resolved
// groups as the coordination reconciler.
func EvaluateUpdateGate(
	ctx context.Context,
	reads client.Reader,
	isvc *v1beta1.InferenceService,
	component v1beta1.ComponentType,
	fromRevision string,
	targetRevision *appsv1.ControllerRevision,
	recorder record.EventRecorder,
	defaults GroupDefaults,
	strategy workloadtypes.UpdateStrategyType,
	inFlightSurge, inFlightUnavail int32,
) (allowed bool, gate v1beta1.RolloutHoldGate, denyReason string) {
	return EvaluateUpdateGateWithPods(ctx, reads, isvc, component, fromRevision, targetRevision, recorder, defaults, strategy, inFlightSurge, inFlightUnavail, nil)
}

// EvaluateUpdateGateWithPods is EvaluateUpdateGate with the Component's own
// pods handed in: the unavailability gate reads them from pods instead of
// listing them through reads, so a caller that consults the gate for several
// Instances in one pass lists the pods once. A nil pods lists at each consult.
func EvaluateUpdateGateWithPods(
	ctx context.Context,
	reads client.Reader,
	isvc *v1beta1.InferenceService,
	component v1beta1.ComponentType,
	fromRevision string,
	targetRevision *appsv1.ControllerRevision,
	recorder record.EventRecorder,
	defaults GroupDefaults,
	strategy workloadtypes.UpdateStrategyType,
	inFlightSurge, inFlightUnavail int32,
	pods PodSource,
) (allowed bool, gate v1beta1.RolloutHoldGate, denyReason string) {
	// The gate context is resolved once so the shared prelude (ResolveGroups +
	// MembershipFor + nil-isvc / no-coord short-circuit) runs once instead of
	// four times -- see the coordination.GateContext docs.
	gateCtx := ResolveGateContextWithDefaults(ctx, reads, isvc, component, defaults)
	gateCtx.TargetRevision = targetRevision
	gateCtx.ComponentPods = pods

	// The plan gate precedes every content gate: with no pinned run there is
	// no validated plan to evaluate the other gates against.
	if gateCtx.Hold {
		return false, v1beta1.RolloutHoldGatePlan, gateCtx.HoldReason
	}

	// Pairing runs first, for EVERY strategy: an in-place update keeps net
	// capacity flat (which is why CheckRatio bypasses it below) but still
	// moves the instance across pairing cohorts — the exact transition the
	// pair floor exists to pace.
	pairing := gateCtx.pairingVerdict(fromRevision, strategy, inFlightSurge, inFlightUnavail)
	if !pairing.allowed {
		return false, v1beta1.RolloutHoldGatePairing, pairing.reason
	}

	// The batch step reads every member's replica; a read it cannot make
	// fails closed like the other gates, under the gate name its hold uses.
	// A pairing peer the pairing gate holds still is not waited on.
	batch, err := gateCtx.readBatchStep()
	if err != nil {
		return false, v1beta1.RolloutHoldGateRatio, err.Error()
	}
	var waived v1beta1.ComponentType
	if pairing.peerHeldStill {
		waived = pairingPeer(component)
	}

	switch gateCtx.Scope {
	case CanaryUnit:
		// A member of a prefill/decode canary unit answers to the pairing
		// gate and to the batch step: the canary executor stages the unit's
		// capacity through each member's partition, and inside a step, as
		// under a revert, the two sides' starts are batched the way a plain
		// roll's are, over the Instances the partition lets move. The ratio
		// band, the group budgets and the Sequential gate read a coordination
		// group and are not consulted; the member's own budget paces its
		// starts.
		if batch.governs {
			if gate, reason := batch.hold(waived); gate != "" {
				return false, gate, reason
			}
		}
		return true, "", ""
	case RecordedGroupPairing:
		// A member of a recorded group the spec no longer declares answers to
		// the pairing gate alone: the member's own budget paces its starts,
		// so the batch step, the ratio band, the group budgets and the
		// Sequential gate are not consulted.
		return true, "", ""
	}

	if gate, reason := gateCtx.ratioHold(batch, waived, strategy, inFlightSurge, inFlightUnavail, recorder); gate != "" {
		return false, gate, reason
	}
	return gateCtx.checkBudgetAndSequential(strategy, inFlightSurge, inFlightUnavail)
}

// ratioHold is the batch step's or the ratio band's hold on a start, and
// "" when neither holds it: the batched members' lockstep when the batch
// step governs (the engine and decoder both step, so the serving band is
// not consulted for them); the serving band, projected by the strategy's
// sign, for a start that changes capacity; and the new-revision lockstep
// alone for an in-place start, whose band waiver is recorded once the start
// is admitted. waived is the pairing peer the pairing gate holds still for
// this start, which neither the batch step nor the lockstep waits on: that
// peer moves only once this Component has, so a wait on it would be a wait
// on this Component's own move.
func (ctx GateContext) ratioHold(batch batchStep, waived v1beta1.ComponentType, strategy workloadtypes.UpdateStrategyType, inFlightSurge, inFlightUnavail int32, recorder record.EventRecorder) (v1beta1.RolloutHoldGate, string) {
	inPlace := strategy == workloadtypes.UpdateStrategyInPlaceIfPossible ||
		strategy == workloadtypes.UpdateStrategyInPlaceOnly
	switch {
	case batch.governs:
		if gate, reason := batch.hold(waived); gate != "" {
			return gate, reason
		}
	case !inPlace:
		// Strategy-aware ratio projection. SurgeThenDrain adds a Ready pod
		// (+1) BEFORE draining the old, so the transient is a capacity GAIN,
		// not a -1 loss. Projecting -1 for a surge roll mis-models it and
		// deadlocks symmetric RatioBalanced groups (both members denied at
		// the start, neither can go first). Drain-first strategies (recreate)
		// keep the honest -1. Both signs apply the new-revision lockstep
		// before the band.
		projDelta := int32(-1)
		if strategy == workloadtypes.UpdateStrategySurgeThenDrain || strategy == "" {
			projDelta = 1
		}
		ok, reason, outsideTolerance := ctx.checkRatio(inFlightSurge, inFlightUnavail, projDelta, waived)
		if !ok {
			return v1beta1.RolloutHoldGateRatio, reason
		}
		// A step no single step could keep inside the tolerance is admitted
		// rather than held for good; the service says so with the numbers.
		if outsideTolerance && recorder != nil && ctx.ISVC != nil {
			recorder.Eventf(ctx.ISVC, corev1.EventTypeWarning, EventReasonRatioStepOutsideTolerance,
				"%s: %s. The serving ratio is outside the tolerance while this step runs; the band admits one such step at a time rather than hold the roll for good. Widen the tolerance or resize the group to a shape one step keeps inside it.",
				EventReasonRatioStepOutsideTolerance, reason)
		}
	default:
		if ok, reason := ctx.checkRatioLockstep(waived); !ok {
			return v1beta1.RolloutHoldGateRatio, reason
		}
		RecordRatioGateBypassed(string(ctx.Component), string(strategy))
		if recorder != nil && ctx.ISVC != nil {
			recorder.Eventf(ctx.ISVC, corev1.EventTypeNormal, EventReasonRatioGateBypassed,
				"RatioGateBypassed: Component=%s, Strategy=%s. The RatioBalanced serving-capacity band is waived for an in-place start because the drain is paired 1:1 with a same-pod return; the new-revision lockstep still applies, and per-Component pacing (CoordinationPacing.PerComponent) still governs concurrent disruption.",
				ctx.Component, strategy)
		}
	}
	return "", ""
}

// checkBudgetAndSequential is the group budget for the start's strategy (the
// surge budget for a surge start, the unavailability budget for a
// drain-first one) and then the Sequential order.
func (ctx GateContext) checkBudgetAndSequential(strategy workloadtypes.UpdateStrategyType, inFlightSurge, inFlightUnavail int32) (bool, v1beta1.RolloutHoldGate, string) {
	if strategy == workloadtypes.UpdateStrategySurgeThenDrain || strategy == "" {
		if ok, reason := ctx.CheckSurge(inFlightSurge); !ok {
			return false, v1beta1.RolloutHoldGateBudget, reason
		}
	} else if ok, reason := ctx.CheckUnavailability(inFlightUnavail); !ok {
		return false, v1beta1.RolloutHoldGateBudget, reason
	}
	if ok, reason := ctx.CheckSequential(); !ok {
		return false, v1beta1.RolloutHoldGateSequential, reason
	}
	return true, "", ""
}

// EvaluatePlanGate is the plan gate alone, the question the workload asks
// for a start it admits without the capacity consult: an Instance that
// serves nothing still rolls on the plan, and until a run pins it no
// grouped Component rolls. A nil service holds nothing, as the full stack
// short-circuits on one.
func EvaluatePlanGate(isvc *v1beta1.InferenceService, component v1beta1.ComponentType) (allowed bool, gate v1beta1.RolloutHoldGate, denyReason string) {
	if isvc == nil {
		return true, "", ""
	}
	if held, reason := planHold(isvc, component); held {
		return false, v1beta1.RolloutHoldGatePlan, reason
	}
	return true, "", ""
}

// GateWaitsOnPeerCounters reports whether a hold on gate is released by a
// peer Component's status moving. The pair floor and the ratio band
// simulate the step against the peers' live serving counts, and a Held
// hold may name a peer whose retry record is Held, which a release or a
// corrected revision on that peer ends; a replica they deny waits on
// exactly those transitions. Every other gate waits on something else: a
// peer's revision or the plan pin (Sequential, Plan), the Component's own
// budget (Budget), or its own retry schedule (RetryBlock).
func GateWaitsOnPeerCounters(gate v1beta1.RolloutHoldGate) bool {
	return gate == v1beta1.RolloutHoldGatePairing || gate == v1beta1.RolloutHoldGateRatio || gate == v1beta1.RolloutHoldGateHeld
}
