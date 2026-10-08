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
// Gate order: pairing -> ratio -> surge | unavailability -> sequential.
// First denial wins; later gates short-circuit. The caller reads the
// allowed bit and may also surface gate + denyReason as a RolloutHold;
// gate names Pairing, Ratio, Budget (surge or unavailability), or
// Sequential accordingly.
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
// that patches the same pod. In-place starts (InPlaceIfPossible / InPlaceOnly) skip
// CheckRatio entirely. For in-place the drain is paired 1:1 with a
// same-pod return (mark-not-ready -> patch -> mark-ready), so the net
// capacity change is ~0; running the gate would project a -1 loss that
// misrepresents that transient as permanent and indefinitely over-blocks
// the smaller Component of a ratio pair (e.g. the decoder in a 4:2
// engine:decoder pair with 25% tolerance deadlocks because 4:1 = 4.0 sits
// above the [1.5, 2.5] band). CheckUnavailability (called below for
// non-surge strategies, which includes both in-place variants) already
// governs concurrent disruption via the MaxUnavailable budget, so
// RatioBalanced safety is preserved without the structural deadlock. The
// RatioGateBypassed event + metric fire so operators see why the gate
// did not run.
//
// Reads authoritative InferenceReplica status via the provided context and
// reader. targetRevision is the ControllerRevision the Component's Instances
// move to in this pass (the rollback revision while a rollback is pinned,
// else the spec target); the pairing gate orients its cohort simulation by
// that revision's protocol. recorder may be nil (the event is best-effort
// observability). defaults carries the operator-configured group fill-ins
// (e.g. the ratio tolerance default) so the gates see the same resolved
// groups as the coordination reconciler.
func EvaluateUpdateGate(
	ctx context.Context,
	reads client.Reader,
	isvc *v1beta1.InferenceService,
	component v1beta1.ComponentType,
	targetRevision *appsv1.ControllerRevision,
	recorder record.EventRecorder,
	defaults GroupDefaults,
	strategy workloadtypes.UpdateStrategyType,
	inFlightSurge, inFlightUnavail int32,
) (allowed bool, gate v1beta1.RolloutHoldGate, denyReason string) {
	// The gate context is resolved once so the shared prelude (ResolveGroups +
	// MembershipFor + nil-isvc / no-coord short-circuit) runs once instead of
	// four times -- see the coordination.GateContext docs.
	gateCtx := ResolveGateContextWithDefaults(ctx, reads, isvc, component, defaults)
	gateCtx.TargetRevision = targetRevision

	// The plan gate precedes every content gate: with no pinned run there is
	// no validated plan to evaluate the other gates against.
	if gateCtx.Hold {
		return false, v1beta1.RolloutHoldGatePlan, gateCtx.HoldReason
	}

	// Pairing runs first, for EVERY strategy: an in-place update keeps net
	// capacity flat (which is why CheckRatio bypasses it below) but still
	// moves the instance across pairing cohorts — the exact transition the
	// pair floor exists to pace.
	if ok, reason := gateCtx.CheckPairing(strategy, inFlightSurge, inFlightUnavail); !ok {
		return false, v1beta1.RolloutHoldGatePairing, reason
	}

	inPlace := strategy == workloadtypes.UpdateStrategyInPlaceIfPossible ||
		strategy == workloadtypes.UpdateStrategyInPlaceOnly
	if !inPlace {
		// Strategy-aware ratio projection. SurgeThenDrain adds a Ready pod
		// (+1) BEFORE draining the old, so the transient is a capacity GAIN,
		// not a -1 loss. Projecting -1 for a surge roll mis-models it and
		// deadlocks symmetric RatioBalanced groups (both members denied at
		// the start, neither can go first). Drain-first strategies (recreate)
		// keep the honest -1.
		projDelta := int32(-1)
		if strategy == workloadtypes.UpdateStrategySurgeThenDrain || strategy == "" {
			projDelta = 1
		}
		if ok, reason := gateCtx.CheckRatio(inFlightSurge, inFlightUnavail, projDelta); !ok {
			return false, v1beta1.RolloutHoldGateRatio, reason
		}
	} else {
		RecordRatioGateBypassed(string(component), string(strategy))
		if recorder != nil && isvc != nil {
			recorder.Eventf(isvc, corev1.EventTypeNormal, EventReasonRatioGateBypassed,
				"RatioGateBypassed: Component=%s, Strategy=%s. RatioBalanced is bypassed for in-place strategies because the drain is paired 1:1 with a same-pod return. Per-Component pacing (CoordinationPacing.PerComponent) still governs concurrent disruption.",
				component, strategy)
		}
	}

	if strategy == workloadtypes.UpdateStrategySurgeThenDrain || strategy == "" {
		if ok, reason := gateCtx.CheckSurge(inFlightSurge); !ok {
			return false, v1beta1.RolloutHoldGateBudget, reason
		}
	} else {
		if ok, reason := gateCtx.CheckUnavailability(inFlightUnavail); !ok {
			return false, v1beta1.RolloutHoldGateBudget, reason
		}
	}

	if ok, reason := gateCtx.CheckSequential(); !ok {
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
// peer Component's serving counters moving. The pair floor and the ratio
// band simulate the step against the peers' live serving counts, so a
// replica they deny waits on exactly those transitions. Every other gate
// waits on something else: a peer's revision or the plan pin (Sequential,
// Plan), the Component's own budget (Budget), or its own retry state
// (RetryBlock, Held).
func GateWaitsOnPeerCounters(gate v1beta1.RolloutHoldGate) bool {
	return gate == v1beta1.RolloutHoldGatePairing || gate == v1beta1.RolloutHoldGateRatio
}
