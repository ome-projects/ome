package canary

import (
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"sigs.k8s.io/ome/pkg/rollout"
)

// EffectivePartition returns the StatefulSet-style partition the controller
// should apply to a component while a canary is in progress, reading the
// current step from status. Instances with index < Partition are held on the
// stable revision; (desired - Partition) roll to the canary revision.
//
// It is a pure read used by the controller to project the partition onto the
// InferenceReplica's rollout-control spec.pacing.partition before the
// component reconcilers run; the engine's partition hold reads it ahead of
// an operator-set lifecycle partition. Returns (nil, false) when no canary
// plan is effective for the Component; policies is the run layer's reading
// of the referenced policies, which the view resolves through outside a run.
//
//   - The current step's Capacity resolved against desired, never below the
//     held floor: the stable revision keeps its last instance until the done
//     sentinel, because it carries traffic until the final write lands and
//     drains through the window after it. A step's capacity is staged on the
//     remaining instances, and the 100% write moves traffic onto the canary
//     capacity that is Ready before the stable instance is released.
//   - A unit stopped Failed holds every Instance on the revision it runs
//     (Partition = desired): the roll stands where the stop found it until a
//     rollback, a resume or a new revision moves it.
//   - A unit with no record that reads RollingBack under a run that records
//     every member at its stable revision holds nothing (Partition 0): its
//     Instances still off that revision roll back to it with no step, after
//     a push the operator took back. A unit that sits such a run out whole
//     reads Stable and keeps step 0's partition, so the next push rolls no
//     more than step 0 stages before its record is written.
//
// Rollback (ome.io/rollout-rollback) is special-cased: when the status records
// a rolled-back revision, this returns Partition 0 (hold nothing). Partition
// alone cannot reverse a roll — the workload skips held instances rather than
// draining their canary pods — so the revert is driven by the IR's
// RollbackToRevision target (it overrides the desired pod template with the
// stable revision and rolls every instance back onto it). A non-zero partition
// here would fight that revert, so it is forced to 0 for the duration.
func EffectivePartition(isvc *v1beta1.InferenceService, policies rollout.Policies, component v1beta1.ComponentType, desiredReplicas int32) (*int32, bool) {
	g := rollout.CanaryGroupFor(isvc, policies, component)
	if g == nil || g.Canary == nil || len(g.Canary.Steps) == 0 {
		return nil, false
	}
	// The canary partition applies only to the canary group's own Component(s).
	if !groupHasComponent(g, component) {
		return nil, false
	}
	plan := g.Canary
	// The group's run state is keyed by its primary; a member of the group's
	// other unit must read the same state to stay in step with it.
	cs := rollout.GroupCanaryStatusFor(isvc, policies, component)
	phase := isvc.Status.Components[rollout.PrimaryOf(g)].RolloutPhase
	// Stopped Failed: hold EVERY Instance where it stands. A stop promises
	// that nothing moves, so no further Instance of any member rolls onto
	// the stopped revision until a rollback, a resume or a new revision;
	// the step's own partition would keep releasing the Instances the step
	// had not reached yet.
	if rollout.StateOf(cs, phase, len(plan.Steps)) == rollout.CanaryStateFailed {
		all := desiredReplicas
		return &all, true
	}
	// Rolled back: hold NOTHING via partition (0). The component is driven back
	// onto the stable revision by the IR's RollbackToRevision target, not by the
	// partition; a non-zero partition here would fight that revert.
	if cs != nil && cs.RolledBackRevisionHash != "" {
		zero := int32(0)
		return &zero, true
	}
	// No record, reading RollingBack, under a run that records every member
	// at its stable revision: the unit has no step to stage, and an
	// Instance still off that revision is rolling back to it after a push
	// the operator took back. Hold NOTHING (0): step 0's partition would
	// keep that Instance where it stands, with the run open on it for good.
	// A unit that sits such a run out whole reads Stable and keeps step 0's
	// partition below, the hold a push lands on before its record is written.
	if cs == nil && phase == v1beta1.RolloutPhaseRollingBack && v1beta1.RolloutRunActive(isvc) && !groupRetargeted(isvc, g, rollout.PrimaryOf(g)) {
		zero := int32(0)
		return &zero, true
	}
	idx := int32(0)
	if cs != nil {
		// Status is an unvalidated subresource: a negative step is an
		// external write, read back from the record before it can index
		// plan.Steps.
		idx = rollout.StepOf(cs, phase, plan.Steps)
	}
	// Done sentinel (CurrentStep past the last step): hold NOTHING, the
	// component rolls entirely onto the canary revision (partition 0). This
	// is the release of the held floor: the final traffic write landed and
	// the drain window elapsed, so the last stable instance rolls now and the
	// unit reads Stable once it has. Don't clamp to the final step and
	// recompute: the floor would strand that instance on the stable revision
	// permanently after completion.
	if int(idx) >= len(plan.Steps) {
		zero := int32(0)
		return &zero, true
	}
	p := stepPartition(plan.Steps[idx], desiredReplicas)
	return &p, true
}

// StepPartition returns the current canary step's partition for a Component,
// the value the projector writes to the InferenceReplica's rollout-control
// spec.pacing.partition so the engine's partition hold stages the split. The
// Component's lifecycle is left untouched: it is the user's update strategy.
// nil when no canary is active for the Component. desiredReplicas is the
// Instance count the Component's replica is scaled to (DesiredReplicas), so
// the partition and the replica count it is stamped beside describe the same
// fleet.
func StepPartition(isvc *v1beta1.InferenceService, policies rollout.Policies, component v1beta1.ComponentType, desiredReplicas int32) *int32 {
	p, ok := EffectivePartition(isvc, policies, component, desiredReplicas)
	if !ok {
		return nil
	}
	return p
}

// PartitionResolver is the rollout-control partition a canary projects for
// a Component, sized by the projector against the Instance count it stamps:
// the current step's hold under an effective plan, else the full plan-gate
// hold. nil when no canary governs the Component.
func PartitionResolver(isvc *v1beta1.InferenceService, policies rollout.Policies, component v1beta1.ComponentType) irprojector.PartitionResolver {
	if !Governs(isvc, policies, component) {
		return nil
	}
	return func(desiredReplicas int32) *int32 {
		if p := StepPartition(isvc, policies, component, desiredReplicas); p != nil {
			return p
		}
		return PlanGateHoldPartition(isvc, policies, component, desiredReplicas)
	}
}

// StableSideTarget is the ControllerRevision name of the revision a replica
// the projection creates for the Component comes up on while its unit keeps
// a stable side: under a standing rollback, the revision the operator rolled
// back to, which every Instance runs while the rejected revision stays the
// spec's; under a live step or a stop, the step's stable revision, because
// a fleet that has run no revision has no side of its own for the step's
// partition to hold it on. The service owns the revision, so it is there to
// come back to. Empty when no canary governs the Component, when its unit
// keeps no stable side (no record, the cutover's release, a stop for want
// of a stable revision), when the run left the member at its stable
// revision, or when no stable revision is recorded for the Component.
func StableSideTarget(isvc *v1beta1.InferenceService, policies rollout.Policies, component v1beta1.ComponentType) string {
	g := rollout.CanaryGroupFor(isvc, policies, component)
	if g == nil || g.Canary == nil || !groupHasComponent(g, component) {
		return ""
	}
	primary := rollout.PrimaryOf(g)
	cs := rollout.GroupCanaryStatusFor(isvc, policies, component)
	if cs == nil || !holdsStableSide(cs, isvc.Status.Components[primary].RolloutPhase, len(g.Canary.Steps)) {
		return ""
	}
	if cs.RolledBackRevisionHash == "" && !memberRetargeted(isvc, component) {
		return ""
	}
	stable := componentStableRevisionHash(isvc, component, primary)
	if stable == "" {
		return ""
	}
	return coordination.ControllerRevisionName(irprojector.RoleReplicaPrefix(isvc, component), component, stable)
}

// holdsStableSide reports whether the unit's state keeps a stable side: a
// standing rollback (RollingBack, RolledBack), a step short of the last
// that holds Instances on the stable revision (Staging, Serving, PreHold),
// or a stop that holds every Instance where it stands (Failed). The final
// step keeps none (its Staging and the drain, Draining): its share is the
// whole fleet, and a member rebuilt on the stable side there would break
// the unit's canary path and rest the other members' traffic on their one
// held Instance for two load rounds. A unit with no record, one whose
// cutover released the last held Instance (Done), and a stop for want of a
// stable revision keep none either.
func holdsStableSide(cs *v1beta1.CanaryStatus, phase v1beta1.RolloutPhase, steps int) bool {
	switch rollout.StateOf(cs, phase, steps) {
	case rollout.CanaryStateRollingBack, rollout.CanaryStateRolledBack,
		rollout.CanaryStateServing, rollout.CanaryStatePreHold:
		return true
	case rollout.CanaryStateStaging:
		return !finalStep(cs, steps)
	case rollout.CanaryStateFailed:
		return !stoppedWithoutStable(cs)
	}
	return false
}

// finalStep reports whether the record stands at its last step, whose
// share is the whole fleet but for the Instances a hold keeps back.
func finalStep(cs *v1beta1.CanaryStatus, steps int) bool {
	return steps > 0 && int(cs.CurrentStep) == steps-1
}

// Governs reports whether a canary governs the Component's partition: an
// effective canary group drives it (StepPartition), or a declared
// canary-kind spec group holds it while no plan is effective
// (PlanGateHoldPartition).
func Governs(isvc *v1beta1.InferenceService, policies rollout.Policies, component v1beta1.ComponentType) bool {
	if isvc == nil {
		return false
	}
	if rollout.CanaryGroupFor(isvc, policies, component) != nil {
		return true
	}
	return declaredCanaryMember(isvc, component)
}

// declaredCanaryMember reports whether the raw spec places the Component in
// a canary-kind group: an inline canary or a policyRef declaring canary.
func declaredCanaryMember(isvc *v1beta1.InferenceService, component v1beta1.ComponentType) bool {
	if isvc.Spec.Rollout == nil {
		return false
	}
	for i := range isvc.Spec.Rollout.Groups {
		g := &isvc.Spec.Rollout.Groups[i]
		if g.DeclaredProgression() == v1beta1.RolloutProgressionCanary && groupHasComponent(g, component) {
			return true
		}
	}
	return false
}

// groupHasComponent reports whether c is a member of the group.
func groupHasComponent(g *v1beta1.RolloutGroup, c v1beta1.ComponentType) bool {
	for _, gc := range g.Components {
		if gc == c {
			return true
		}
	}
	return false
}

// PlanGateHoldPartition returns a full-hold partition (every instance held
// on its current revision) for a Component that belongs to a canary-KIND
// spec group — an inline canary or a policyRef declaring canary — while NO
// effective canary plan is resolvable (a reference whose policy cannot be
// resolved). It is the projection-side twin of the update gates' plan hold:
// the projected spec stays DETERMINISTIC across the pre-open and parked
// states, so a transiently lost pin cannot flap the projected IR between a
// step partition and no partition (which churns the IR generation and
// starves every fresh-snapshot gate downstream). nil when a canary plan IS
// effective (StepPartition owns that) or the Component is not in a
// canary-kind group. desiredReplicas is the Instance count the Component's
// replica is scaled to: a hold at the declared minimum would release every
// Instance an autoscaler added above it.
func PlanGateHoldPartition(isvc *v1beta1.InferenceService, policies rollout.Policies, component v1beta1.ComponentType, desiredReplicas int32) *int32 {
	if rollout.CanaryGroupFor(isvc, policies, component) != nil || !declaredCanaryMember(isvc, component) {
		return nil
	}
	return &desiredReplicas
}
