package coordination

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadstatus "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
)

// conditionReady is the IR-level condition type written by the
// InferenceReplica status writer (inferencereplica.InferenceReplicaConditionReady).
// Duplicated as a local const because the inferencereplica package
// imports coordination — importing it back here would be a cycle.
const conditionReady = "Ready"

// CheckSequentialGate is the consumer side of Sequential coordination. The
// OMENative ops dispatcher calls it before it starts a fresh per-Instance
// Update, so the Sequential invariant, at most one Component rolls at a
// time in the operator-declared Order, holds on the action side and is not
// only observed on the status side. The state machine names the member
// whose turn it is; the dispatcher starts an Update wherever
// detectUpdateTrigger finds one due, so without this gate every member
// whose spec changed in one pass would roll at once and the group's phase
// would never read one member rolling while the others wait.
//
// Returns allowed=true when:
//   - The Component is not in any coordination group,
//   - The Component's group Policy is not Sequential (gate doesn't apply),
//   - The Component is the active Sequential Component (its turn), AND
//     no wait after the previous Component's finish is running,
//   - No Component in the group has a rollout in flight (idle group).
//
// Returns allowed=false when:
//   - A different Component in the Order is currently rolling (the
//     active selector picks that one; this Component must wait).
//   - The Component IS the active one but the operator-configured wait
//     between the previous Component's completion and this one's
//     start is still in effect.
//
// An Instance already mid-update is not gated: detectUpdateTrigger admits
// it so in-flight updates are not stranded. The dispatcher consults this
// gate on a fresh start only, like CheckRatioGate and
// CheckUnavailabilityGate.
//
// Reads authoritative InferenceReplica status via the provided context and
// reader.
func CheckSequentialGate(ctx context.Context, reads client.Reader, isvc *v1beta1.InferenceService, component v1beta1.ComponentType) (allowed bool, reason string) {
	return ResolveGateContext(ctx, reads, isvc, component).CheckSequential()
}

// CheckSequential is the GateContext-aware variant of
// CheckSequentialGate. See GateContext / CheckRatio for the pre-resolve
// pattern the dispatcher uses to pay the prelude once per per-Instance
// tick.
func (ctx GateContext) CheckSequential() (allowed bool, reason string) {
	if ctx.ShortCircuit {
		return true, ctx.ShortReason
	}
	group := ctx.Group
	if group.Policy != v1beta1.CoordinationPolicySequential {
		return true, "not Sequential pacing"
	}
	if len(group.Order) == 0 {
		// Sequential without an Order is malformed (ValidateGroupShape
		// refuses it); the gate steps aside rather than hold forever.
		return true, "Sequential group missing Order"
	}
	// A read error fails closed: a zero-valued observation for the rolling
	// peer would admit a second Component and break at-most-one-rolling.
	observations, err := observeSequentialComponentsForGate(ctx.Ctx, ctx.Reads, ctx.ISVC, group.Components)
	if err != nil {
		return false, fmt.Sprintf("cannot read Sequential group IR status, failing closed: %v", err)
	}
	active, hasActive := activeSequentialComponent(group.Order, observations)
	if !hasActive {
		// No Component in flight: the group is idle and nothing is held.
		return true, "Sequential group idle"
	}
	if active != ctx.Component {
		return false, fmt.Sprintf("Sequential waiting on %s", active)
	}
	if held, why := sequentialWaitAfterPrevious(ctx.ISVC, group, observations, ctx.Component); held {
		return false, why
	}
	return true, "Sequential active Component"
}

// sequentialWaitAfterPrevious holds the active Component while the group's
// wait after the previous Component's finish is still running, or while
// that Component is not Ready. The wait is measured from the finish the
// group's status records, else from the previous Component's Ready
// transition; with neither, nothing is measured and the Component goes.
// The state machine holds the group's phase over the same window, so both
// sides read one clock that a later loss of readiness, or a recreated
// replica, neither cuts short nor restarts. Both reasons carry stable
// facts only (the Components and the configured duration): a live
// countdown would change every pass and defeat the hold's Since.
func sequentialWaitAfterPrevious(isvc *v1beta1.InferenceService, group ResolvedGroup, observations map[v1beta1.ComponentType]ComponentObservation, component v1beta1.ComponentType) (bool, string) {
	if group.Soak <= 0 {
		return false, ""
	}
	prev := previousCompletedComponentForGate(group.Order, observations, component)
	if prev == "" {
		return false, ""
	}
	completed := recordedMemberFinishAt(isvc, group.Name, prev)
	if completed.IsZero() {
		completed = observations[prev].ReadyAt
	}
	if completed.IsZero() {
		return false, ""
	}
	if observations[prev].NotReady {
		return true, fmt.Sprintf("Sequential.Soak: %s held while %s is not Ready; it starts once %s is Ready and the %s soak after %s has elapsed", component, prev, prev, group.Soak, prev)
	}
	if time.Since(completed) < group.Soak {
		return true, fmt.Sprintf("Sequential.Soak: %s waiting out the %s soak after %s", component, group.Soak, prev)
	}
	return false, ""
}

// inFlightSignals are the three readings of one member's replica that each
// say its roll is in flight. Any one of them keeps the member in flight;
// together they keep "at most one Component rolls" true across the windows
// in which a replica's status trails its spec, or its spec trails the
// service. Reading a member as in flight when its pod template did not
// change is harmless: the dispatcher finds nothing to update on it and
// never asks the gate, and the reading clears once the projector and the
// status writer catch up.
type inFlightSignals struct {
	// revisionSkew: the status names an update revision other than the
	// current one. It reads true once the status records the new target
	// and stays true until the roll's last Instance moves.
	revisionSkew bool
	// statusLag: the status was not written for the pod templates the spec
	// holds. Between the projector's spec write and the status write that
	// records the new target, the update revision still names the old one;
	// this reading covers that window. It compares the template hash the
	// projector writes beside the spec, not the generation: a spec write
	// that changes no template (an annotation the revision leaves out, a
	// count, a pause, a partition) leaves the status current. The template
	// hash says nothing about the parent service; that is projectionLag.
	statusLag bool
	// projectionLag: the parent-generation annotation the projector writes
	// on every pass differs from the live service generation, or is absent.
	// The service controller projects the group's replicas one at a time,
	// so between the operator's change and the end of that pass a peer can
	// read fully settled while its new spec is not written yet. The reading
	// clears on the projector's next pass, also on a member whose projected
	// spec did not change, which is what lets a later member in the Order
	// start without waiting on an idle earlier one. Equality is strict: an
	// annotation from another generation, on either side, means the service
	// and the replica in hand are not one consistent snapshot.
	projectionLag bool
}

// inFlight reports whether the member's roll is in flight by any reading.
func (s inFlightSignals) inFlight() bool {
	return s.revisionSkew || s.statusLag || s.projectionLag
}

// rowsCurrent reports whether the replica's rows describe the push its
// status records: the update revision differs from the current one, and
// the status was written for the spec the replica holds and the projection
// for the service in hand. Only then does the staged-shape reading mean
// anything. A push lands on the replica's spec first and the status records
// it one write later; until then the rows still sit at the previous push's
// staged shape and would read as done for the new push, and the next member
// would start before this one moved. With no skew the member is not in
// flight at all, and under a lag reading it stays in flight until the
// status catches up and the rows say where it stands.
func (s inFlightSignals) rowsCurrent() bool {
	return s.revisionSkew && !s.statusLag && !s.projectionLag
}

// observeSequentialComponentsForGate reads one ComponentObservation per
// member from authoritative replica status: the revision hashes, the Ready
// reading, whether the roll is in flight (inFlightSignals) and whether it
// stands at its staged shape. A member with no replica reads as a
// zero-valued observation (NotReady, not in flight), which is what the
// active selector needs. Mirrors buildComponentObservation's revision
// compare without the pod-count and desired-replicas fields the gate does
// not consult.
func observeSequentialComponentsForGate(ctx context.Context, reads client.Reader, isvc *v1beta1.InferenceService, components []v1beta1.ComponentType) (map[v1beta1.ComponentType]ComponentObservation, error) {
	out := make(map[v1beta1.ComponentType]ComponentObservation, len(components))
	for _, c := range components {
		// One consistent replica snapshot per member: metadata (generation,
		// parent-generation annotation), spec (the effective partition: the
		// projected pacing partition, else the merged service and runtime
		// lifecycle partition, a runtime-inherited one included) and status.
		// A read error fails closed: a zero-valued observation for the
		// rolling peer would admit a second Component and break
		// at-most-one-rolling.
		ir, _, err := irprojector.DecodedComponentIRFor(ctx, reads, isvc, c)
		if err != nil {
			return nil, err
		}
		partition := irprojector.IRPartition(ir)
		// A replica that is gone publishes no Ready condition: NotReady, as
		// the group's own observation reads it.
		obs := ComponentObservation{Component: c, Partition: partition, NotReady: true}
		if ir != nil {
			summary := &ir.Status
			cur := query.RevisionFromName(summary.CurrentRevision)
			tgt := query.RevisionFromName(summary.UpdateRevision)
			obs.CurrentRevisionHash = cur.Hash()
			obs.TargetRevisionHash = tgt.Hash()
			obs.ReadyAt = readyTransitionAt(summary.Conditions)
			obs.NotReady = readyConditionFalseOrAbsent(summary.Conditions)
			signals := inFlightSignals{
				revisionSkew:  !tgt.IsZero() && !tgt.Same(cur),
				statusLag:     !StatusObservedTemplates(ir),
				projectionLag: irParentGenerationMismatch(ir, isvc.Generation),
			}
			obs.RolloutInFlight = signals.inFlight()
			// The staged-shape reading says the member's roll is done: every
			// Instance the partition lets the roll move is Ready on the
			// update revision and the held ones are Ready on their own.
			if signals.rowsCurrent() {
				obs.AtDesiredShape = workloadstatus.ReachedDesiredShape(
					v1beta1convert.InstanceStatusSliceToWorkload(summary.InstanceStatuses),
					summary.UpdateRevision, partition, summary.Replicas)
			}
		}
		out[c] = obs
	}
	return out, nil
}

// irParentGenerationMismatch reports whether the replica's parent-generation
// annotation differs from the live service generation: the projection the
// replica carries was not produced from the service spec the gate decides
// for, so its settled-looking status cannot be trusted. Equality is strict:
// an annotation behind the service means the projector has not re-applied
// this Component yet, one ahead of it means the gate holds a stale service
// read, and neither pair is one consistent snapshot. A missing or
// unparsable annotation fails closed (a mismatch): a coordination gate must
// not admit a peer on the strength of an annotation that is not there.
func irParentGenerationMismatch(ir *v1beta1.InferenceReplica, isvcGeneration int64) bool {
	raw, ok := ir.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey]
	if !ok {
		return true
	}
	gen, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return true
	}
	return gen != isvcGeneration
}

// previousCompletedComponentForGate names the Component before c in the
// order when that Component's roll is done: not in flight, or at its
// staged shape (every Instance the partition lets the roll move is Ready
// on the target, the held ones Ready on their own revision). A staged
// Component keeps its current revision behind its update revision by
// design, so "in flight" alone would never name it and the wait after
// readiness would never be measured behind a partition. The reading is
// the one activeSequentialComponent skips a done Component by, so the
// Component the wait is measured after is the one the gate stopped
// waiting on. Empty for the first Component in the order, and while the
// previous Component is still rolling: the gate then names that Component
// as the active one and no wait comes into it.
func previousCompletedComponentForGate(order []v1beta1.ComponentType, observations map[v1beta1.ComponentType]ComponentObservation, c v1beta1.ComponentType) v1beta1.ComponentType {
	for i, oc := range order {
		if oc != c {
			continue
		}
		if i == 0 {
			return ""
		}
		prev := order[i-1]
		if pobs, ok := observations[prev]; ok && pobs.RolloutInFlight && !pobs.AtDesiredShape {
			return ""
		}
		return prev
	}
	return ""
}

// recordedMemberFinishAt returns the instant the service's status records
// the named group's member prev done, zero when the group's entry carries
// no record or records another member.
func recordedMemberFinishAt(isvc *v1beta1.InferenceService, groupName string, prev v1beta1.ComponentType) time.Time {
	if isvc == nil || isvc.Status.RolloutCoordination == nil {
		return time.Time{}
	}
	for _, gs := range isvc.Status.RolloutCoordination.Groups {
		if gs.Name != groupName || gs.MemberFinished == nil || gs.MemberFinished.Component != prev {
			continue
		}
		return gs.MemberFinished.At.Time
	}
	return time.Time{}
}
