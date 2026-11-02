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

// conditionReady is the IR-level condition type stamped by the
// InferenceReplica status writer (inferencereplica.InferenceReplicaConditionReady).
// Duplicated as a local const because the inferencereplica package
// imports coordination — importing it back here would be a cycle.
const conditionReady = "Ready"

// CheckSequentialGate is the consumer side of Sequential coordination.
// The OMENative ops dispatcher calls it before kicking off a fresh
// per-Instance Update so the Sequential invariant — "at most one
// Component rolls at a time, in the operator-declared Order" — is
// enforced on the action side, not just observed on the status side.
//
// Without this gate, both Components of a 2-Component Sequential group
// would recreate concurrently the moment the operator bumps both images:
// the Sequential state machine correctly emits `decoder.Surging` then
// `engine.Surging`, but the dispatcher fires Update on whichever
// Instance's `detectUpdateTrigger` returns true, regardless of which
// Component the state machine considers active. The composite phase
// `decoder.Idle` / `Sequential.Awaiting` never lands in status because
// engine starts surging before decoder finishes.
//
// Returns allowed=true when:
//   - The Component is not in any coordination group,
//   - The Component's group Policy is not Sequential (gate doesn't apply),
//   - The Component is the active Sequential Component (its turn), AND
//     no soak window is currently held,
//   - No Component in the group has a rollout in flight (idle group).
//
// Returns allowed=false when:
//   - A different Component in the Order is currently rolling (the
//     active selector picks that one; this Component must wait).
//   - The Component IS the active one but the operator-configured soak
//     window between the previous Component's completion and this one's
//     start is still in effect.
//
// The "already mid-update" branch in detectUpdateTrigger intentionally
// bypasses gates so in-flight updates aren't stranded; the dispatcher
// only calls this gate on startingFresh, matching CheckRatioGate /
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
	isvc := ctx.ISVC
	component := ctx.Component
	if group.Policy != v1beta1.CoordinationPolicySequential {
		return true, "not Sequential pacing"
	}
	if len(group.Order) == 0 {
		// Sequential without an Order is malformed (ValidateGroupShape
		// would have rejected this). Bypass rather than wedge — the
		// group already has a louder problem.
		return true, "Sequential group missing Order"
	}

	// Compute per-Component RolloutInFlight observations from authoritative
	// IR status — mirrors buildComponentObservation's revision-hash check
	// but on the much smaller subset the gate needs. A read error fails
	// closed: fabricating a zero-valued observation for the rolling peer
	// would admit a second Component and break at-most-one-rolling.
	observations, err := observeSequentialComponentsForGate(ctx.Ctx, ctx.Reads, isvc, group.Components)
	if err != nil {
		return false, fmt.Sprintf("cannot read Sequential group IR status, failing closed: %v", err)
	}
	active, hasActive := activeSequentialComponent(group.Order, observations)
	if !hasActive {
		// No Component in flight at all — the group is idle (the
		// operator hasn't bumped anything yet, or the wake-up is racing
		// the very first observation). Don't block; the moment any
		// Component flips RolloutInFlight, the gate will route the
		// dispatcher to the first Order entry.
		return true, "Sequential group idle"
	}
	if active != component {
		return false, fmt.Sprintf("Sequential waiting on %s", active)
	}

	// active == component: this Component IS its turn. Honor the soak
	// window between completion of the previous Component and the start
	// of this one. The state machine's sequentialTransition holds the
	// composite phase at Sequential.Awaiting during the soak; the gate
	// mirrors that on the dispatcher side so the per-Instance Update
	// doesn't fire while the operator's soak is still elapsing.
	//
	// Clock: the finish the group's status records for the previous
	// Component, the instant the group first read its roll done and the
	// clock the group's own phase reads, so both sides measure one window
	// that the previous Component losing readiness afterwards, or its
	// replica being recreated, neither cuts short nor starts over. While
	// the group has not recorded the finish yet, the previous Component's
	// IR Ready condition transition stands in: Ready sits at Unknown
	// (RolloutInProgress) while it rolls and turns True when its roll is
	// done, every Instance on the target or, under a partition, at the
	// staged shape. The group's LastTransitionTime is the wrong source: it
	// only updates when the base Phase changes, and Sequential's soak
	// window holds the group at Phase=Idle
	// (CompositePhase=Sequential.AwaitingNextComponent), so it points back
	// at the pre-rollout Idle and time.Since(entered) is always past
	// soakDuration.
	//
	// The previous Component must also stand Ready: while its Ready
	// condition is False or absent, this Component is held whatever the
	// window says, and the window keeps running, so the recovery releases
	// this Component at once when the window has elapsed by then. Both
	// reasons carry stable facts only (components, the configured
	// duration): a live countdown would change every reconcile and defeat
	// the Since preservation on status.rolloutHold (storms the apiserver).
	if group.Soak > 0 {
		if prev := previousCompletedComponentForGate(group.Order, observations, component); prev != "" {
			completed := recordedMemberFinishAt(isvc, group.Name, prev)
			if completed.IsZero() {
				completed = observations[prev].ReadyAt
			}
			if !completed.IsZero() {
				if observations[prev].NotReady {
					return false, fmt.Sprintf("Sequential.Soak: %s held while %s is not Ready; it starts once %s is Ready and the %s soak after %s has elapsed", component, prev, prev, group.Soak, prev)
				}
				if time.Since(completed) < group.Soak {
					return false, fmt.Sprintf("Sequential.Soak: %s waiting out the %s soak after %s", component, group.Soak, prev)
				}
			}
		}
	}

	return true, "Sequential active Component"
}

// observeSequentialComponentsForGate builds the per-Component
// RolloutInFlight observations the gate needs from authoritative IR
// status. Mirrors buildComponentObservation's revision-hash compare
// but skips the pod-count / desired-replicas fields the gate doesn't
// consult. Returns one ComponentObservation per `components` entry —
// Components with no status block get a zero-valued observation
// (RolloutInFlight=false), which is exactly what the active-selector
// needs.
//
// RolloutInFlight is derived from THREE independent signals:
//
//  1. Revision-hash skew: `UpdateRevision != CurrentRevision` — the
//     classic "spec changed and status caught up enough to record the
//     new target name" signal. This is what fires once the deferred
//     AggregateAndWriteStatus has stamped UpdateRevision=target.Name on
//     the Component being reconciled.
//
//  2. Status lag: the status was not written for the pod templates the
//     spec holds (StatusObservedTemplates) — fires at the *moment of
//     bump*, after the projector has written the IR spec but BEFORE the
//     IR's status has recorded the new target. In that window
//     UpdateRevision still names the OLD CR (so signal #1 is silent)
//     even though the Component is about to roll. The compare is
//     against the template hash the projector wrote beside the spec,
//     not the generation: a spec write that changes no template (an
//     annotation value the revision leaves out, a count, a pause, a
//     partition) leaves the status current. The template hash says nothing
//     about the parent ISVC — that is signal #3's job.
//
//  3. Projection lag: the parent-generation annotation the projector
//     stamps on every pass differs from the live isvc.Generation (or
//     is absent). The ISVC controller projects the group's IRs one at
//     a time, so between the operator's bump and the completion of
//     that projection pass a peer IR can look fully converged while
//     its new spec simply hasn't been written yet. Failing closed
//     here keeps "at most one Component rolls" honest across that
//     window; the signal self-clears the moment the projector
//     re-stamps the annotation — including on Components whose
//     projected spec did not change at all (a partial bump), which is
//     what lets the gate release a later-in-Order Component without
//     waiting on an idle earlier one. Strict equality again: a stamp
//     from a different generation — either side — means the ISVC and
//     IR in hand are not one consistent snapshot.
//
// Over-detection from signals #2/#3 is harmless. A Component whose
// target PodSpec didn't actually change still gets observed as
// in-flight here, but detectUpdateTrigger in the dispatcher returns
// needs=false for that Component (its pods' RunningRevision already
// matches target.Name) and the gate is never consulted. Once the
// projector and the IR status writer catch up, the noise clears.
func observeSequentialComponentsForGate(ctx context.Context, reads client.Reader, isvc *v1beta1.InferenceService, components []v1beta1.ComponentType) (map[v1beta1.ComponentType]ComponentObservation, error) {
	out := make(map[v1beta1.ComponentType]ComponentObservation, len(components))
	for _, c := range components {
		// One consistent IR snapshot per Component: metadata (generation,
		// parent-generation annotation), spec (effective partition: the
		// projected pacing partition, else the merged ISVC↔runtime lifecycle
		// partition — including a runtime-inherited one the raw ISVC never
		// shows), and status. A read error
		// fails closed: fabricating a zero-valued observation for the
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
			revisionSkew := !tgt.IsZero() && !tgt.Same(cur)
			statusLag := !StatusObservedTemplates(ir)
			projectionLag := irParentGenerationMismatch(ir, isvc.Generation)
			obs.RolloutInFlight = revisionSkew || statusLag || projectionLag
			// Convergence to the desired staged shape — mirrors
			// buildComponentObservation so the gate's active-selector sees
			// the same "done for handoff" signal the state machine does.
			//
			// Gate on revisionSkew: AtDesiredShape means "converged to the
			// NEW target's staged shape", which is undefined until status
			// records that target. During the lag windows (status or
			// projection one pass behind a fresh bump) UpdateRevision still
			// names the OLD CR, so ReachedDesiredShape compares
			// instances-on-old against target-old and spuriously reports
			// convergence. That released the Sequential gate the moment both
			// images were bumped — every Component saw RolloutInFlight=true
			// (via lag) AND AtDesiredShape=true (via stale target) at once,
			// the active-selector found nothing active, and all Components
			// started rolling at t=0 (overlap). Only trust AtDesiredShape
			// once the target is genuinely known to differ.
			if revisionSkew {
				obs.AtDesiredShape = workloadstatus.ReachedDesiredShape(
					v1beta1convert.InstanceStatusSliceToWorkload(summary.InstanceStatuses),
					summary.UpdateRevision, partition, summary.Replicas)
			}
		}
		out[c] = obs
	}
	return out, nil
}

// irParentGenerationMismatch reports whether the IR's parent-generation
// annotation differs from the live ISVC generation — the projection this
// IR carries was not produced from the ISVC spec the gate is deciding
// for, so its converged-looking status cannot be trusted. Strict
// equality on purpose: a stamp trailing the ISVC means the projector
// hasn't re-applied this Component yet, and a stamp ahead of it means
// the gate is holding a stale ISVC read; neither pair is a consistent
// snapshot. A missing or unparsable annotation fails closed (treated as
// a mismatch): coordination gates must not admit a peer on the strength
// of a stamp that isn't there.
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
